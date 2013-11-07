package com.cs.integrating.impl;

import com.cs.integrating.core.TikaExtract;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.*;
import java.util.Iterator;
import java.util.Map;

/**
 * User: luo fang
 * Date: 13-11-4
 * Func: Upload file From Local To Hadoop platform
 */
public class UploadData {

    private static Configuration hbaseConf = HBaseConfiguration.create();

    private static final String NON_ALPHA_NUM_REGEX = "[^\\dA-Za-z\u3007\u3400-\u4DB5\u4E00-\u9FCB\uE815-\uE864]";

    //3 table to store unstructured data
    protected static final String DOC = "doc_table";

    protected static final String MEDIA = "media_table";

    protected static final String PIC = "pic_table";

    protected static final String OTHER = "other_table";

    static String[] cfs = {"sourceInfo", "attributes"};

    protected void create(HBaseAdmin admin, String tableName, String[] todoCF) throws IOException {

        HTableDescriptor desc = new HTableDescriptor(tableName);

        for (String cf: todoCF) {
            desc.addFamily(new HColumnDescriptor(cf));
        }
        admin.createTable(desc);
        //admin.enableTable(tableName);
        admin.close();
    }

    public void createInitTable() throws IOException {

        HBaseAdmin admin = new HBaseAdmin(hbaseConf);

        if (!admin.tableExists(DOC)){
            create(admin, DOC, cfs);
        }
        if (!admin.tableExists(MEDIA)) {
           create(admin, MEDIA, cfs);
        }
        if (!admin.tableExists(OTHER)) {
            create(admin, OTHER, cfs);
        }

        if (!admin.tableExists(PIC)) {
            create(admin, PIC, cfs);
        }

        admin.close();
    }


    protected String getFileType(String srcFile) throws IOException, TikaException, SAXException {

        InputStream is = new BufferedInputStream(new FileInputStream(srcFile));

        ContentHandler handler = new BodyContentHandler();

        Metadata metadata = new Metadata();
        metadata.set(Metadata.RESOURCE_NAME_KEY, srcFile);

        Parser parser = new AutoDetectParser();
        ParseContext context = new ParseContext();
        context.set(Parser.class, parser);

        parser.parse(is, handler, metadata, context);
       /* String[] names = metadata.names();
        for (String name:names) {
            if (metadata.get(name) != null) {
                System.out.println(name+":"+metadata.get(name));
            }
        }*/
        return metadata.get("Content-Type");
    }

    protected String getDestTable(String fileName) {

        int dotPos = fileName.lastIndexOf(".");
        if (dotPos < 0 || dotPos >= fileName.length()-1) return OTHER;

        String type = fileName.substring(dotPos+1).trim();

        if (type.matches("doc|wps|txt|pdf|ppt|xls"))  {
            return DOC;
        } else if (type.matches("bmp|jpg|tiff|gif|pcx|tga|exif|fpx|svg|psd|cdr|pcd|dxf|ufo|eps|ai|raw")) {
            return PIC;
        } else if (type.matches("mpeg|mpg|dat|avi|mov|asf|wmv|navi|mkv|flv|rmvb|"))  {
            return MEDIA;
        } else {
            return OTHER;
        }
    }

    protected Put constructPut(Put put, byte[] cf, byte[] col, byte[] val) {

        put.add(cf, col, 0, val);
        return put;
    }

    public void uploadFromLocalToHbase(String localPath) throws TikaException, IOException, SAXException {

        File localFile = new File(localPath);

        InputStream is = new BufferedInputStream(new FileInputStream(localFile));
        int bytesNum = is.available();
        System.out.println("file byte num: " + bytesNum);

        byte[] fileBytes = new byte[bytesNum];    //将文本的二进制写入hbase
        is.read(fileBytes);

        String fileName = localFile.getName();
        System.out.println("fileName: " + fileName);

        String destTable = getDestTable(fileName);

        String rowkey = localPath.replaceAll(NON_ALPHA_NUM_REGEX, "");

        System.out.println("unreversed rowkey: " + rowkey);

        StringBuffer sb = new StringBuffer(rowkey);
        rowkey = sb.reverse().toString();
        System.out.println("reversed rowkey: " + rowkey);

        HTable hTable = new HTable(hbaseConf, destTable);
        Put put = new Put(rowkey.getBytes());

        //cf1
        constructPut(put, cfs[0].getBytes(), "content".getBytes(), fileBytes);
        constructPut(put, cfs[0].getBytes(), "name".getBytes(), fileName.getBytes());

        //cf2
        TikaExtract te = new TikaExtract();
        Map<String, String> kv = te.extractDoc(localPath);
        Iterator<String> attrKeys = kv.keySet().iterator();
        while (attrKeys.hasNext()) {
            String key = attrKeys.next();
            String val = kv.get(key);
            constructPut(put, cfs[1].getBytes(), key.getBytes(), val.getBytes());
        }

        hTable.put(put);
        hTable.close();
    }

    public void upload(String baseDir) throws TikaException, IOException, SAXException {
        //使用while循环扫描本地文件
        File file = new File(baseDir);
        if (!file.exists()) {
            System.out.println("baseDir is not exist");
            return;
        }

        if (file.isFile()) {
            uploadFromLocalToHbase(baseDir);
            return;
        } else {
            File[] files = file.listFiles();
            for (File subFile:files) {
                upload(subFile.getPath());
            }
            return;
        }
    }

    public static void main(String[] args) throws TikaException, IOException, SAXException {
        UploadData ud = new UploadData();
        ud.createInitTable();

        String baseDir = args[0];
        ud.upload(baseDir);

    }
}
