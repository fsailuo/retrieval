package com.cs.mycrawler.impl;

import com.cs.mycrawler.core.CrawlerMappper;
import com.cs.mycrawler.core.LinkMappper;
import com.cs.mycrawler.core.LinkReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * User: luo fang
 * Date: 13-10-21 Time: 上午11:19
 * func: get webpage data from website with mapreduce framework
 */

public class MyCrawlerJob extends Configured implements Tool{

    private static Logger LOG = Logger.getLogger(MyCrawlerJob.class);

    private static Configuration hbaseConf = HBaseConfiguration.create();

    private String todo_table = "sina_news_todo";

    private String visited_table = "sina_news_visited";

    private boolean delTable(String tableName)  {
        try {
            HBaseAdmin admin = new HBaseAdmin(hbaseConf);
            if (admin.tableExists(tableName)) {
                System.out.println("delete table: " + tableName + "......");
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
            admin.close();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    protected void creat1Table(String tableName, String[] todoCF) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(hbaseConf);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);

        HTableDescriptor desc = new HTableDescriptor(tableName);
        for (String cf: todoCF) {
            desc.addFamily(new HColumnDescriptor(cf));
        }
        admin.createTable(desc);
        admin.enableTable(tableName);
        admin.close();
    }

    //删除已存在的表，并创建新表
    private void createTable(String todo, String visited) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(hbaseConf);
        if (admin.tableExists(todo)) {
            System.out.println("todo table already existed");
            admin.disableTable(todo);
            admin.deleteTable(todo);
        }

        if (admin.tableExists(visited)) {
            System.out.println("visited table already existed");
            admin.disableTable(visited);
            admin.deleteTable(visited);
        }

        //column family defined
        String[] todoCF = {"attr"};
        String[] visitedCF = {"html", "attr", "cmnt"};

        //create todo_table
        HTableDescriptor desc = new HTableDescriptor(todo);
        for (String cf: todoCF) {
            desc.addFamily(new HColumnDescriptor(cf));
        }
        admin.createTable(desc);
        admin.enableTable(todo);

        //create_visited
        desc = new HTableDescriptor(visited);
        for (String cf: visitedCF) {
            desc.addFamily(new HColumnDescriptor(cf));
        }
        admin.createTable(desc);
        admin.enableTable(visited);
        admin.close();
    }

    private void initTable() throws IOException {
        //创建todo表和visited表
      /*
        Configuration conf = new Configuration();
        conf.addResource("configure.xml");

        String todo_table = conf.get("table.todo_table");

        String visited = conf.get("table.visited_table");
        String[] base_urls = conf.getStrings("lf.base.url");
        */
        String[] base_urls = {"http://news.sina.com.cn/"};
        createTable(todo_table, visited_table);

        //将base_url加入todo表中, timestamp表示是第几轮循环
        HTable hTable = new HTable(hbaseConf, todo_table);
        List<Put> puts = new ArrayList<Put>();

        for (String url : base_urls) {
            Put put = new Put(url.getBytes(), 0);
            put.add("attr".getBytes(), null, null);
            puts.add(put);

        }

        hTable.put(puts);
        hTable.close();
    }

    public Job createSubmittableJob(int tag) throws IOException {
        Scan  scan = new Scan();
        scan.setCaching(1024);
        Job job = null;

        if (tag == 0) {
            // config htmlJob
            job = new Job(hbaseConf, "htmljob");
            job.setJarByClass(MyCrawlerJob.class);
            TableMapReduceUtil.initTableMapperJob(todo_table, scan, CrawlerMappper.class, ImmutableBytesWritable.class, Put.class, job);
            TableMapReduceUtil.initTableReducerJob(visited_table, null, job);
            job.setNumReduceTasks(0);
        } else {
            job = new Job(hbaseConf, "linkjob");
            job.setJarByClass(MyCrawlerJob.class);
            TableMapReduceUtil.initTableMapperJob(visited_table, scan, LinkMappper.class, Text.class, IntWritable.class, job);
            TableMapReduceUtil.initTableReducerJob(todo_table, LinkReducer.class, job);
        }

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
         System.out.println("Todo: initTable......");
        //创建并插入初始数据
        initTable();

        System.out.println("Todo: configure Job......");
        //配置两个作业
        Job getHtmlJob = createSubmittableJob(0);
        Job getLinkJob = createSubmittableJob(1);

        System.out.println("Todo: getHtml Job......");
        boolean res = getHtmlJob.waitForCompletion(true);

        System.out.println("Todo: empty todo_table");
        //TODO:empty todo_table
        boolean isdel = delTable(todo_table);
        if (!isdel) {
            System.out.println("run func: delete todo table failed");
            return 1;
        }

        String[] todoCF = {"attr"};
        creat1Table(todo_table, todoCF);

        System.out.println("Todo: getLinkJob......");
        res = getLinkJob.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) {
        try {
            int res = ToolRunner.run(new Configuration(), new MyCrawlerJob(), args);
        } catch (Exception e) {
            LOG.info("MyCrawler Job run exception");
        }
    }
}
