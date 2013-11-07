package com.cs.mycrawler.core;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * User: luo fang
 * Date: 13-10-23
 * Func: realize crawl and extract webpage func with hadoop mapper
 */

public class CrawlerMappper extends  TableMapper<ImmutableBytesWritable, Put> {

    private static Logger log = Logger.getLogger(CrawlerMappper.class);

    private static GetWebData gwd = new GetWebData();

    public void map(ImmutableBytesWritable row, Result val, Context context) throws IOException, InterruptedException {

        String url = new String(val.getRow());
        String page = gwd.getPage(url);
        Map<String, String> attrMap = gwd.getSplitData(page, url);
        Put put = getPut(url, page, attrMap);
        context.write(row, put);
    }

    private static Put getPut(String url, String content, Map<String, String> attrsMap) {

        Put put = new Put(url.getBytes());
        put.add("html".getBytes(), "content".getBytes(), 0, content.getBytes());

        byte[] cf = "attr".getBytes();

        String title = attrsMap.get(WebAttrsEnum.TITLE);
        if (title != null) {
            put.add(cf, WebAttrsEnum.TITLE.getBytes(), 0, title.getBytes());
        }

        String pubTime = attrsMap.get(WebAttrsEnum.CREATE_TIME);
        if (pubTime != null) {
            put.add(cf, WebAttrsEnum.CREATE_TIME.getBytes(), 0, pubTime.getBytes());
        }

        String media = attrsMap.get(WebAttrsEnum.MEDIA);
        if (media != null) {
            put.add(cf, WebAttrsEnum.MEDIA.getBytes(), 0, media.getBytes());
        }

        String body = attrsMap.get(WebAttrsEnum.BODY);
        if (body != null) {
            put.add(cf, WebAttrsEnum.BODY.getBytes(), 0, body.getBytes());
        }

        String cls = attrsMap.get(WebAttrsEnum.CLASS);
        if (cls != null) {
            put.add(cf, WebAttrsEnum.CLASS.getBytes(), 0, cls.getBytes());
        }

        String author = attrsMap.get(WebAttrsEnum.AUTOR);
        if (author != null) {
            put.add(cf, WebAttrsEnum.AUTOR.getBytes(), author.getBytes());
        }

        String outDegree = attrsMap.get(WebAttrsEnum.OUTDEGREE);
        if (outDegree != null) {
            put.add(cf, WebAttrsEnum.OUTDEGREE.getBytes(), outDegree.getBytes());
        }

        return put;
    }

    /*
    private void addEleToPut(Put put, String col, String val) {
    } */
}