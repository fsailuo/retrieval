package com.cs.mycrawler.core;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Iterator;

/**
 * User: luo fang
 * Date: 13-10-30
 * Fun: remove  duplicate links
 */
public class LinkReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable>{

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int tag = 1;
        Iterator<IntWritable> vals = values.iterator();
        while (vals.hasNext()) {
            if (vals.next().get() == 0) {
                tag = 0;
                break;
            }
        }
        if (tag == 1) {
            //not visited
            Put put = new Put(key.getBytes());
            put.add("attr".getBytes(), null, null);
            context.write(null, put);
        } else return;

    }

}
