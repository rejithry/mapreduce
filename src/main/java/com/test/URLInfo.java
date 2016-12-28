package com.test;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by raghr010 on 12/27/16.
 */
public class URLInfo implements WritableComparable<URLInfo> {

    public URLInfo() {
        this.url = new Text();
        this.pageRank = new FloatWritable();
    }

    public URLInfo(Text url, FloatWritable pageRank) {
        this.url = url;
        this.pageRank = pageRank;
    }

    private Text url;
    private FloatWritable pageRank;

    @Override
    public int compareTo(URLInfo o) {
        return o.url.compareTo(this.url);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        url.write(dataOutput);
        pageRank.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
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

    @Override
    public boolean equals(Object other) {
        return this.url.equals(((URLInfo)other).url);
    }

    @Override
    public int hashCode() {
        return this.url.hashCode();
    }

}
