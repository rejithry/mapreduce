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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by raghr010 on 12/25/16.
 */
public class CommonFriends extends Configured implements Tool {

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new CommonFriends(), args);
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

        job.setJobName("CommonFriends");
        job.setJarByClass(CommonFriends.class);
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
            String[] users = value.toString().split(",");

            String user = users[0];
            List<String> friends = Arrays.asList(users).subList(1, users.length);

            for (int  i = 0; i < friends.size(); i++) {
                String otherFriends = "";
                for (int j = 0; j < friends.size(); j ++) {
                    if (i != j) {
                        otherFriends += friends.get(j) + ",";
                    }
                }

                otherFriends = otherFriends.substring(0, otherFriends.length()-1);

                String outputKey = "";

                if (user.compareTo(friends.get(i)) < 0) {
                    outputKey += user + "," + friends.get(i);
                } else {
                    outputKey += friends.get(i) + user;
                }

                context.write(new Text(outputKey),  new Text(otherFriends));
            }

        }
    }


    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> listOfOtherFriends, Context context) throws IOException, InterruptedException {

            List<String> mutualFriendList = new ArrayList<>();
            List<String> otherFriendsOfA = null;
            List<String> otherFriendsOfB = null;

            for (Text t : listOfOtherFriends) {
                if (otherFriendsOfA == null){
                    otherFriendsOfA = Arrays.asList(t.toString().split(","));
                } else {
                    otherFriendsOfB = Arrays.asList(t.toString().split(","));
                }
            }
            otherFriendsOfA.retainAll(otherFriendsOfB);
            mutualFriendList = otherFriendsOfA;

            String mutualFriendListString = "";

            for (String friend : mutualFriendList){
                mutualFriendListString += friend + ",";
            }

            if (mutualFriendListString.length() > 0){
                mutualFriendListString = mutualFriendListString.substring(0, mutualFriendListString.length()-1);
            }

            context.write(key, new Text(mutualFriendListString));

        }
    }
}
