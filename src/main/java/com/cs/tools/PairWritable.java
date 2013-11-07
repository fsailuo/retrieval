package com.cs.tools;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: luo fang
 * Date: 13-10-30
 * Func: generate an PairWritable
 */
public class PairWritable implements Writable{

    private String html = null;

    //0,visited;1,tod;2,undefined
    private int tag = 0;

    public PairWritable(int tag) {
         new PairWritable(null, tag);
    }

    public PairWritable(String page) {
         new PairWritable(page, 2);
    }

    public PairWritable(String page, int tag) {
        setHtml(page);
        setTag(tag);
    }

    public int getTag() {
        return tag;
    }

    public void setTag(int tag) {
        this.tag = tag;
    }

    public String getHtml() {
        return html;
    }

    public void setHtml(String html) {
        this.html = html;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        Text.writeString(dataOutput, html);
        dataOutput.write(tag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.html = Text.readString(dataInput);
        this.tag = dataInput.readInt();
    }
}
