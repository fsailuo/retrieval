package com.cs.mycrawler.core;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * User: Luo fang
 * Date: 13-10-23
 * Func: get webdata and web split data
 */

public class GetWebData {

    public static final Logger LOG = Logger.getLogger(GetWebData.class);

    public  static HttpClient client = GetWebClient.getClientInstance();

    //获取网页数据
    public String getPage(String uri) throws IOException {

        GetMethod method = GetWebClient.getMethodInstance(uri);
        return getPage(client, method);
    }

    public String getPage(HttpClient client, GetMethod method) throws IOException {

        int statCode = client.executeMethod(method);
        if (statCode != HttpStatus.SC_OK) {
             LOG.error("http execute getMethod failed");
            return null;
        }

        //获取网页数据内容
        byte[] webBytes = method.getResponseBody();
        String charset = "gb2312";
        //String charset = method.getRequestCharSet();
       // System.out.println("charset: " + charset);

        method.releaseConnection();

        String content = new String(webBytes, charset);
        return content;
    }

    //解析网页链接links,并只返回有效的链接
   public Set<String> getLinks(String html, String baseUrl) {
       Document doc = Jsoup.parse(html, baseUrl);
       Elements eles = doc.select("a[href]");

      // System.out.println("links Num: " + eles.size());

       Set<String> links = new HashSet<String>();

       //过滤掉非正常的url并将相对路径的url转化为绝对路径
       for (Element e: eles) {
           String ele = new String(e.attr("href").trim());
           if (!ele.isEmpty()) {
               if (ele.startsWith("http") || ele.startsWith("./")) {
                   if (ele.startsWith("./")) {
                    //   System.out.println("ele: " + ele);
                        ele = baseUrl.substring(0, baseUrl.lastIndexOf("/")) + ele.substring(1);
                   }
                   if (ele.indexOf("news.sina") >= 0) {
                       links.add(ele);
                   }
               }
           }
       }

      /*
      for (String link : links) {
        System.out.println(link);
      }
       System.out.println("effect links num: " + links.size());   */

       return links;
   }

    //jsoup解析网页数据获取相关属性信息，
    // TODO: 注意属性为null的情况待处理
    public HashMap<String, String> getSplitData(String html, String baseUrl) {

        HashMap<String, String> attrs = new HashMap<String, String>();

        Document doc = Jsoup.parse(html, baseUrl);

        String title = getTitle(doc);
        System.out.println("title: " + title);
        if (title != null) {
            attrs.put(WebAttrsEnum.TITLE, title);
        }

        String charSet = getCharSet(doc);
        System.out.println("charSet: " + charSet);
        if (charSet != null) {
            attrs.put(WebAttrsEnum.ENCODE, charSet);
        }

        String pubDate = getTime(doc);
        if (pubDate != null) {
            pubDate = pubDate.replaceAll("年|月|日| |：|:", "-");
        }
        System.out.println("date: " + pubDate);
        if (pubDate != null) {
            attrs.put(WebAttrsEnum.CREATE_TIME, pubDate);
        }

        String media = getMedia(doc);
        System.out.println("media: " + media);
        if (media != null) {
            attrs.put(WebAttrsEnum.MEDIA, media);
        }

        String body = getBody(doc);
        System.out.println("body: " + body);
        if (body != null) {
            attrs.put(WebAttrsEnum.BODY, body);
        }

        String cls = getClassification(doc);
        System.out.println("cls: " + cls);
        if (cls != null) {
            attrs.put(WebAttrsEnum.CLASS, cls);
        }

        String author = getAuthor(doc);
        System.out.println("author: " + author);
        if (author != null) {
            attrs.put(WebAttrsEnum.AUTOR, author);
        }

        int linkNum = getLinkNum(doc);
        System.out.println("linkNum: " + linkNum);
        if (linkNum != 0) {
            attrs.put(WebAttrsEnum.OUTDEGREE, new Integer(linkNum).toString());
        }
        return attrs;
    }

    //not impl
    private String getCharSet(Document doc) {
        return "gb2312";
    }

    //获取文章的主题
    private String getTitle(Document doc) {
        Element ele = doc.getElementById("artibodyTitle");
        if (ele == null) return null;
        return ele.text();
    }

    //获取该网页的出度
   private int getLinkNum(Document doc) {
       Elements eles = doc.select("a[href]");
       if (eles == null) return 0;
       else  return eles.size();
   }

    //获取创建的时间
    private String getTime(Document doc) {
        Element ele = doc.getElementById("pub_date");
        if (ele == null) return null;
        return ele.text();
    }

    //获取文章分类
    private String getClassification(Document doc) {
        return doc.title();
    }

    //获取发布媒体信息
    private String getMedia(Document doc) {
        Element ele = doc.getElementById("media_name");
        if (ele == null) return null;
        return ele.text();
    }

    //获取正文内容
    private String getBody(Document doc) {
        Element ele = doc.getElementById("artibody");
        if (ele == null) return null;

        Elements bodyEles = ele.getElementsByTag("p");
        if (bodyEles == null) return null;

       // System.out.println("p Num: "  + bodyEles.size());
        StringBuffer sb = new StringBuffer();
        for (Element e: bodyEles) {
            if (e.text() != null) {
                sb.append(e.text());
            }
        }
        return sb.toString();
    }

    //获取正文中的图片信息来源    not finished yet
    private String getPic(Document doc) {
        Element ele = doc.getElementById("artibody");
        Elements picEles = ele.getElementsByAttribute("img");
        if (picEles == null) return null;

        System.out.println("pic size: " + picEles.size());
        String[] picLinks = new String[picEles.size()];
        for (Element pic : picEles) {

        }
        return null;
    }

    private String getVideo() {
        return null;
    }

    private String getAuthor(Document doc) {
        String text = doc.text();
        int start = text.indexOf("（编辑");
        if (start == -1) return null;
        text = text.substring(start);
        int end = text.indexOf("）");
        if (end == -1 || end > 10) return null;
        String author = text.substring(3, end);
        return author;
    }

    //htmlClient中没有，需要使用htmlUnit才能获取
    private String getCmntsParts(Document doc) {
        Elements eles = doc.select(".f_red");
        if (eles == null) {
            System.out.println("eles is null");
            return null;
        }
        System.out.print("ele Num: " + eles.size());
        for (Element cmnt : eles) {
            System.out.println("cmnt: " + cmnt.text());
        }
        return null;
    }

}
