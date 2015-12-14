package hadoop;

import java.io.IOException;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
public class MapFileSplit {
 
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        
    	private final static IntWritable one = new IntWritable(1);
 
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            
                context.write(new Text(line), one);
        }
    } 
 
 
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
//        org.apache.log4j.BasicConfigurator.configure();
 
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "WordCount");
        job.setJarByClass(MapFileSplit.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
        job.setMapperClass(Map.class);
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        TextInputFormat.setMaxInputSplitSize(job, 1);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.out.println(TextInputFormat.getMaxSplitSize(job));
 
        job.waitForCompletion(true);
    }
}