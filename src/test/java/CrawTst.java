import com.cs.mycrawler.core.GetWebClient;
import com.cs.mycrawler.core.GetWebData;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * User: luo fang
 * Date: 13-10-26
 * Func: crawler func test
 */

public class CrawTst {

    private static GetWebData gwd = new GetWebData();

    public void writeLocal(String content, String url) throws IOException {
        String fileName = "local";
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File("D:\\docwww\\"+fileName)));
        bw.write(content);
        bw.close();
    }

    public boolean crawlData(String url) throws IOException {

          /*  HtmlPage hp = GetWebClient.getPage(url);
        System.out.println("encoding: " + hp.getPageEncoding());
        String content = hp.asXml();    */

        String content = gwd.getPage(url);
        System.out.println("url content: " + content);
        gwd.getLinks(content, url);
        gwd.getSplitData(content, url);
        writeLocal(content, url);
        return false;

    }

    public static void main(String[] args) {
        CrawTst crawTst = new CrawTst();
       // String tstUrl = "http://roll.news.sina.com.cn/news/gnxw/gdxw1/index.shtml";
        String tstUrl = "http://mil.news.sina.com.cn/2013-10-28/0752746585.html";
        try {
            crawTst.crawlData(tstUrl);
        } catch (IOException e) {
            //e.printStackTrace();
            System.out.println("connet error:");
            System.exit(1);
        }
    }

}
