package com.test;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by raghr010 on 12/27/16.
 */
public class URLOrPageRank implements Writable {

    private Text tag;
    private Text url;

    public URLOrPageRank(Text tag, Text url, FloatWritable pageRank) {
        this.tag = tag;
        this.url = url;
        this.pageRank = pageRank;
    }

    private FloatWritable pageRank;


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        tag.write(dataOutput);
        url.write(dataOutput);
        pageRank.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        tag.readFields(dataInput);
        url.readFields(dataInput);
        pageRank.readFields(dataInput);
    }

    public Text getUrl() {
        return url;
    }

    public void setUrl(Text url) {
        this.url = url;
    }

    public FloatWritable getPageRank() {
        return pageRank;
    }

    public void setPageRank(FloatWritable pageRank) {
        this.pageRank = pageRank;
    }

    @Override
    public String toString() {
        return url.toString() + ":" + pageRank.toString();
    }

    public Text getTag() {
        return tag;
    }

    public void setTag(Text tag) {
        this.tag = tag;
    }
}
