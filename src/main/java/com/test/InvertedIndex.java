package com.test;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import java.util.Set;

/**
 * Created by raghr010 on 12/25/16.
 */
public class InvertedIndex extends Configured implements Tool {

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new InvertedIndex(), args);
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

        job.setJobName("InvertedIndex");
        job.setJarByClass(InvertedIndex.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setNumReduceTasks(1);

        return job.waitForCompletion(true) ? 0 : 1;

    }



    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            Text documentId = new Text(parts[0]);
            Set<String> contents = Map.findWordsFromContent(parts[1]);

            for (String word : contents) {
                context.write(new Text(word), documentId);
            }
        }

        private static Set<String> findWordsFromContent(String content) {
            // TODO more sophisticated logic to strip out the stop words, include bigrams etc.
            return Sets.newHashSet(content.split(" "));
        }
    }


    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String documentList = "";

            for (Text t : values) {
                documentList += t.toString() + ",";
            }
            documentList = documentList.substring(0, documentList.length()-1);
            context.write(key, new Text(documentList));

        }
    }
}
