/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 13-11-5
 * Time: 下午4:25
 * To change this template use File | Settings | File Templates.
 */

import com.cs.tools.*;

import java.util.Iterator;
import java.util.List;

public class SplitSentenceInstance {

    public static void main(String[] args) {
        String test = "京华时报２００８年1月23日报道 昨天，受一股来自中西伯利亚的强冷空气影响，本市出现大风降温天气，白天最高气温只有零下7摄氏度，同时伴有6到7级的偏北风";
        SplitWord sw = new SplitWord("D:\\docwww\\mmseg4j-1.8.5\\data");
        List<String> words = sw.splitWord(test);
        Iterator<String> wordIte = words.iterator();
        while (wordIte.hasNext()) {
            System.out.println(wordIte.next());
        }
    }
}
