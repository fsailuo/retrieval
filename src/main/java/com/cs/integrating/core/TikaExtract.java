package com.cs.integrating.core;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.microsoft.ooxml.OOXMLParser;
import org.apache.tika.parser.pdf.PDFParser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * User: luo fang
 * Date: 13-11-3
 * Func: extract diff type docment to plain text
 */
public class TikaExtract {

    public void getMIMEParserType(){

        TikaConfig tc = TikaConfig.getDefaultConfig();
        Set<MediaType> mediaType = tc.getParsers().keySet();
        Iterator<MediaType> mtIte = mediaType.iterator();

        while (mtIte.hasNext()) {
            System.out.println("media Type: " + mtIte.next().getType());
        }
    }

    public Map<String, String> extractDoc(String srcFile) throws IOException, TikaException, SAXException {

        HashMap<String, String> attrsMap = new HashMap<String, String>();

        InputStream is = new BufferedInputStream(new FileInputStream(srcFile));

        ContentHandler handler = new BodyContentHandler();

        Metadata metadata = new Metadata();
        metadata.set(Metadata.RESOURCE_NAME_KEY, srcFile);

        Parser parser = new AutoDetectParser();
        ParseContext context = new ParseContext();
        context.set(Parser.class, parser);

        parser.parse(is, handler, metadata, context);
     //   System.out.println("metadata: " + metadata.toString());

        //获取文件的纯文本内容信息、
        String content = handler.toString();
        if (content != null) {
            attrsMap.put("docBody", content);
        }

        //获取文件的元数据信息
        String[] metaName = metadata.names();
        for (String name : metaName) {
            String val = metadata.get(name);
            if (val != null) {
                attrsMap.put(name, val);
            }
        }
        is.close();
        return attrsMap;
    }

    public Map<String, String> extractUrlDoc(String uri) {

        return null;
    }

    public void extractPDF(String srcFile) throws IOException, TikaException, SAXException {
        InputStream is = new BufferedInputStream(new FileInputStream(srcFile));

        ContentHandler handler = new BodyContentHandler();

        Metadata metadata = new Metadata();
        System.out.println("---------------------");
        for (String name: metadata.names()) {
            System.out.print(name + "|");
        }
        System.out.println("---------------------");

        metadata.set(Metadata.RESOURCE_NAME_KEY, srcFile);
        PDFParser pdfParser = new PDFParser();
        pdfParser.parse(is, handler, metadata);
       // System.out.printf("content: " + handler.toString());
        System.out.printf("metadata: " + metadata.toString());
        String[] metaName = metadata.names();
        for (String name: metaName) {
            System.out.print(name + "|");
        }
    }



    public static void main(String[] args) throws TikaException, IOException, SAXException {
        TikaExtract te = new TikaExtract();
       // te.getMIMEParserType();
      //  String fullText = new String(te.extractDoc("D:\\docwww\\Lucene In Action中文第2版.pdf"));
       // System.out.println(fullText);
        te.extractPDF("D:\\\\docwww\\\\Lucene In Action中文第2版.pdf");
    }
}
