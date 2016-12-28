package com.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import com.google.common.primitives.Bytes;

/**
 * Created by raghr010 on 12/25/16.
 */
public class PageRank extends Configured implements Tool {

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new PageRank(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path intermediatePath = new Path("/tmp/pagerank/");

        Configuration conf = getConf();
        Job job = new Job(conf, this.getClass().toString());

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, intermediatePath);

        job.setJobName("PageRankPrepare");
        job.setJarByClass(PageRank.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setMapOutputKeyClass(URLInfo.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(URLInfo.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MapPrepare.class);
        job.setNumReduceTasks(0);

        int r = job.waitForCompletion(true) ? 0 : 1;
        if (r == 1) {
            return 1;
        }

        Job job2 = new Job(conf, this.getClass().toString() + "job2");

        FileInputFormat.setInputPaths(job2, intermediatePath);
        FileOutputFormat.setOutputPath(job2, outputPath);

        job2.setJobName("PageRank");
        job2.setJarByClass(PageRank.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(URLOrPageRank.class);
        job2.setOutputKeyClass(URLInfo.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapperClass(Map.class);
        job2.setReducerClass(Reduce.class);
        job2.setNumReduceTasks(1);

        return job2.waitForCompletion(true) ? 0 : 1;
    }

    public static class MapPrepare extends Mapper<LongWritable, Text, URLInfo, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String []urls = value.toString().split(",");

            Text allUrls = new Text("");

            byte [] comma = ",".getBytes();
            int lengthOfComma = comma.length;
            int count = 0;
            for (String url : urls) {
                count += 1;
                if (count == 1) {
                    continue;
                }
                byte [] bytes = url.getBytes();

                if (count == urls.length) {
                    allUrls.append(bytes, 0, bytes.length);
                } else {
                    allUrls.append(Bytes.concat(bytes, comma), 0, bytes.length + lengthOfComma);
                }
            }

            context.write(new URLInfo(new Text(urls[0]), new FloatWritable(1.0f)), allUrls );

        }

    }

    public static class Map extends Mapper<URLInfo, Text, Text, URLOrPageRank> {

        public void map(URLInfo key, Text value, Context context) throws IOException, InterruptedException {
            String[] outUrls = value.toString().split(",");

            for (String outUrl : outUrls) {
                context.write(new Text(outUrl), new URLOrPageRank(new Text("pageRank"), new Text(), new FloatWritable(key.getPageRank().get()/outUrls.length)));
                context.write(new Text(key.getUrl()), new URLOrPageRank(new Text("url"), new Text(outUrl), new FloatWritable()));
            }
        }
    }


    public static class Reduce extends Reducer<Text, URLOrPageRank, URLInfo, Text> {

        public void reduce(Text key, Iterable<URLOrPageRank> values, Context context) throws IOException, InterruptedException {
            List<Text> outputUrls = new ArrayList<Text>();
            float pageRank = 0;

            for (URLOrPageRank urlOrPageRank :values) {
                if (urlOrPageRank.getTag().equals("url")){
                    outputUrls.add(urlOrPageRank.getUrl());
                } else {
                    pageRank += urlOrPageRank.getPageRank().get();
                }
            }

            Text allUrls = new Text("");

            byte [] comma = ",".getBytes();
            int lengthOfComma = comma.length;
            int count = 0;
            for (Text t : outputUrls) {
                count += 1;
                byte [] bytes = t.getBytes();

                if (count == outputUrls.size()) {
                    allUrls.append(bytes, 0, bytes.length);
                } else {
                    allUrls.append(Bytes.concat(bytes, comma), 0, bytes.length + lengthOfComma);
                }
            }

            context.write(new URLInfo(key, new FloatWritable(pageRank)), allUrls);

        }
    }
}
