package com.cs.mycrawler.core;


import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

/**
 * User: luo fang
 * Date: 13-10-30
 * Fun: get links for next-step crawler
 */
public class LinkMappper extends TableMapper<Text, IntWritable> {

    private static GetWebData gwd = new GetWebData();

    private  Text text = new Text();

    private  IntWritable baseTag = new IntWritable(0);

    private  IntWritable otherTag = new IntWritable(1);

    public void map(ImmutableBytesWritable row, Result val, Context context) throws IOException, InterruptedException {
        String baseUrl = new String(val.getRow());
        String content = new String(val.getValue("html".getBytes(), "content".getBytes()));

        Set<String> urls = gwd.getLinks(content, baseUrl);
        text.set(baseUrl);
        context.write(text, baseTag);
        Iterator<String> others = urls.iterator();
        while (others.hasNext()) {
            text.set(others.next());
            context.write(text, otherTag);
        }
    }

}
