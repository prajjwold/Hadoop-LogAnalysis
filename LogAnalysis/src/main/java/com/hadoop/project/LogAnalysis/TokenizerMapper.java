package com.hadoop.project.LogAnalysis;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TokenizerMapper extends Mapper<Text, Text, Text, LongWritable> {

	private final static LongWritable one = new LongWritable(1);
	private Text word = new Text();

	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString(),"^");
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			context.write(word, one);
		}
	}
}
