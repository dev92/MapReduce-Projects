package com.hadoop.hw2;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
 
public class PerTaskTally {
	
 
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();
        private HashMap<String, Integer> H;
        private MapReduceJobs m = new MapReduceJobs();
        
        @Override
        public void setup(Context context){
        	H = new HashMap<String,Integer>();
        }
 
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
            	String temp = tokenizer.nextToken();
            	if(m.filterToken(temp)){
            		int count = H.containsKey(temp) ? H.get(temp) : 0;
            		H.put(temp, count + 1);
            	}
            }
        }
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{
        	for(String s:H.keySet()){
        		word.set(s);
        		context.write(word, new IntWritable(H.get(s)));
        	}
        }
    }
    
    public static class customPartitioner extends Partitioner<Text,IntWritable>{
        
    	public int getPartition(Text key, IntWritable value, int numReduceTasks){
        if(numReduceTasks==0)
            return 0;
        else if(key.toString().toLowerCase().startsWith("m")){
        	return 0;
        }else if(key.toString().toLowerCase().startsWith("n")){
        	return 1;
        }else if(key.toString().toLowerCase().startsWith("o")){
        	return 2;
        }else if(key.toString().toLowerCase().startsWith("p")){
        	return 3;
        }else if(key.toString().toLowerCase().startsWith("q")){
        	return 4;
        }
		return 0;
    }
   }
 
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
    
    
}
