package com.cs.tools;

import com.chenlb.mmseg4j.*;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * User: luo fang
 * Date: 13-11-5
 * Func:split words to offer support of indexing and search
 */
public class SplitWord {

    private static SplitWord splitInstance = null;

    protected Dictionary dic;

    protected String dicPath = "D:\\docwww\\mmseg4j-1.8.5\\data";

    private SplitWord() {
        dic = Dictionary.getInstance(dicPath);
    }

    public SplitWord(String dictPath) {
        dic = Dictionary.getInstance(dictPath);
    }

    public static SplitWord getSplitInstance() {
        if (splitInstance == null) {
            splitInstance = new SplitWord();
        }
        return splitInstance;
    }

    protected Seg getSeg() {
        return new ComplexSeg(dic);
    }

    //get words constructing the sentence
     public List<String> splitWord(String sentence) {

         StringReader strReader = new StringReader(sentence);

         Seg seg = getSeg();       //取得不同的分词算法
         MMSeg mmSeg = new MMSeg(strReader, seg);
         Word word = null;

         ArrayList<String> wordArr = new ArrayList<String>();

         try {
             while ((word = mmSeg.next()) != null) {
                 wordArr.add(word.getString());
             }
             if (wordArr.isEmpty()) return null;
             else return wordArr;

         } catch (IOException e) {
            return null;
         }
     }
}
