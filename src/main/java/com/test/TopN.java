package com.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by raghr010 on 12/25/16.
 */
public class TopN extends Configured implements Tool {

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new TopN(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Configuration conf = getConf();
        Job job = new Job(conf, this.getClass().toString());

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setJobName("TopN");
        job.setJarByClass(TopN.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(TopMapper.class);
        job.setReducerClass(TopReducer.class);
        job.setNumReduceTasks(1);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class TopMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        private int size;
        private PriorityQueue<User> priorityQueue;

        public TopMapper() {
            this.priorityQueue = new PriorityQueue<User>();
            this.size = 5;
        }


        public void map(LongWritable key, Text value, Context context) {
            int followers = Integer.parseInt(value.toString().split(",")[1]);
            String name = value.toString().split(",")[0];

            if (this.priorityQueue.isEmpty()){
                priorityQueue.add(new User(name, followers));
            } else if (followers > this.priorityQueue.peek().getFollowers()) {
                priorityQueue.add(new User(name, followers));
            }
            if (priorityQueue.size() > this.size){
                priorityQueue.poll();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            while (priorityQueue.size() != 0) {
                context.write(NullWritable.get(), new Text(priorityQueue.poll().toString()));
            }
        }

    }

    public static class TopReducer extends Reducer<NullWritable, Text, Text, NullWritable> {

        private int size;
        private PriorityQueue<User> priorityQueue;

        public TopReducer() {
            this.size = 5;
            this.priorityQueue = new PriorityQueue<User>();
        }
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v : values){
                String[] keyValue = v.toString().split(",");
                String name = keyValue[0];
                int followers = Integer.parseInt(keyValue[1]);

                if (this.priorityQueue.isEmpty()){
                    priorityQueue.add(new User(name, followers));
                } else if (followers > this.priorityQueue.peek().getFollowers()) {
                    priorityQueue.add(new User(name, followers));
                }

                if (priorityQueue.size() > this.size){
                    priorityQueue.poll();
                }
            }

            while (priorityQueue.size() != 0) {
                context.write(new Text(priorityQueue.poll().toString()), NullWritable.get());
            }
        }
    }
}
