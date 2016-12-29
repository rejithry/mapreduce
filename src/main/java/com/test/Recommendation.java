package com.test;

import com.google.common.primitives.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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
import java.util.*;

/**
 * Created by raghr010 on 12/25/16.
 */
public class Recommendation extends Configured implements Tool {

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new Recommendation(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path intermediatePath = new Path("/tmp/intermediate");

        Configuration conf = getConf();
        Job job = new Job(conf, this.getClass().toString());

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, intermediatePath);

        job.setJobName("RecommendationPrepare");
        job.setJarByClass(Recommendation.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(RecommendationPrepareMap.class);
        job.setReducerClass(RecommendationPrepareReduce.class);
        job.setNumReduceTasks(1);

        int r = job.waitForCompletion(true) ? 0 : 1;
        if (r == 1) {
            return 1;
        }

        Job job2 = new Job(conf, this.getClass().toString() + "job2");

        FileInputFormat.setInputPaths(job2, intermediatePath);
        FileOutputFormat.setOutputPath(job2, outputPath);

        job2.setJobName("Recommendation");
        job2.setJarByClass(Recommendation.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(MapWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapperClass(RecoMapper.class);
        job2.setReducerClass(RecoReducer.class);
        job2.setNumReduceTasks(1);

        return job2.waitForCompletion(true) ? 0 : 1;
    }

    public static class RecommendationPrepareMap extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String []parts = value.toString().split(",");

            Text user = new Text(parts[0]);

            String allItems = "";
            List<String> items = Arrays.asList(parts).subList(1, parts.length);

            for (String item : items) {
                allItems += item + ",";
            }

            if (allItems.length() > 0) {
                allItems = allItems.substring(0, allItems.length()-1);
            }

            context.write( user, new Text(allItems));

        }

    }

    public static class RecommendationPrepareReduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Set<String> items = new HashSet<>();

            for (Text item : values) {
                items.add(item.toString());
            }

            String allItems = "";
            for (String item : items) {
                allItems += item + ",";
            }

            if (allItems.length() > 0) {
                allItems = allItems.substring(0, allItems.length()-1);
            }

            context.write(key, new Text(allItems) );

        }

    }

    public static class RecoMapper extends Mapper<Text, Text, Text, MapWritable> {

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] allItems = value.toString().split(",");

            HashMap<String, Integer> itemCounts = new HashMap<>();
            for (String item : allItems) {
                itemCounts.put(item, 1);
            }

            for (String item : allItems) {
                MapWritable coOccuringItems = new MapWritable();
                for (Map.Entry<String, Integer> entry : itemCounts.entrySet()) {
                    if (!entry.getKey().equals(item)) {
                        coOccuringItems.put(new Text(entry.getKey()), new IntWritable(1));
                    }
                }

                context.write(new Text(item), coOccuringItems);
            }
        }
    }


    public static class RecoReducer extends Reducer<Text, MapWritable, Text, Text> {

        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {

            MapWritable coOccuringItems = new MapWritable();

            for (MapWritable map : values) {
                for (MapWritable.Entry entry : map.entrySet()) {
                    if (coOccuringItems.containsKey(entry.getKey())) {
                        coOccuringItems.put((Text)entry.getKey(), new IntWritable(((IntWritable)(coOccuringItems.get(entry.getKey()))).get()
                                + ((IntWritable)entry.getValue()).get()));
                    } else {
                        coOccuringItems.put((Text)entry.getKey(), (IntWritable)entry.getValue());
                    }
                }
            }

            String coOccuringItemsString = "";
            for (MapWritable.Entry entry : coOccuringItems.entrySet()) {
                coOccuringItemsString += ((Text)entry.getKey()).toString() + ":" + String.valueOf(((IntWritable)entry.getValue()).get()) + ",";

            }

            if (coOccuringItemsString.length() > 0) {
                coOccuringItemsString = coOccuringItemsString.substring(0, coOccuringItemsString.length()-1);
            }

            context.write(key, new Text(coOccuringItemsString));

        }
    }
}
