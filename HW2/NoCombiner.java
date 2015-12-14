package com.hadoop.hw2;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

 
public class NoCombiner {
 
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private MapReduceJobs m = new MapReduceJobs();
 
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
            	String temp = tokenizer.nextToken();
            	if(m.filterToken(temp)){
            		word.set(temp);
            		context.write(word, one);
            	}
            }
        }
    }
    
    public static  class customPartitioner extends Partitioner<Text,IntWritable>{
        
    	@Override
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
		return 5;
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
