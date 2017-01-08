package com.sid.TruliaChallenge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.util.StringTokenizer;


/**
 * Created by srathi on 12/28/16.
 */
public class PartitionList extends Configured implements Tool {
        static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                StringTokenizer st2 = new StringTokenizer(line, ",");
                while (st2.hasMoreElements()) {
                   int val = Integer.parseInt((String)st2.nextElement());
                   if (val %2  == 0)
                   context.write(new Text("A"),new IntWritable(val));
                    else
                       context.write(new Text("B"),new IntWritable(val));
                }
            }
        }


    static class MaxTemperatureReducer extends Reducer<Text, IntWritable,  IntWritable, NullWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntWritable val : values) {
                context.write(val, NullWritable.get());
            }
        }
    }



        @Override
        public int run(String[] args) throws Exception {
            Configuration conf = new Configuration();
            conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

            Job job = new Job(conf);
            if (job == null) {
                return -1;
            }
            job.setJarByClass(PartitionList.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            TextOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setMapperClass(MaxTemperatureMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
//            job.setPartitionerClass(FirstPartitioner.class);
            //job.setSortComparatorClass(KeyComparator.class);
//            job.setGroupingComparatorClass(GroupComparator.class);
            job.setReducerClass(MaxTemperatureReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(NullWritable.class);
            //job.setNumReduceTasks(1);
            return job.waitForCompletion(true) ? 0 : 1;
        }

            public static void main(String[] args) throws Exception {
            int exitCode = ToolRunner.run(new PartitionList(), args);
            System.exit(exitCode);
        }
    }
