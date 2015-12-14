package com.mapreduce.hw4;

import java.io.IOException;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.opencsv.CSVParser;


 
public class MonthFlightDelay {
	
	/*
	 * The Map() checks if its a valid record and creates a Custom key
	 * based on FlightCode and Month, this custom key class is explained in
	 * FlightMonthkey.java and the value emitted is ArrDelayMins. 
	 */
	
	public static class Map extends Mapper<LongWritable, Text, FlightMonthKey, Text> {
        
		private Text delay = new Text();
        
        CSVParser csvparser = new CSVParser(',');

                
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String cols[] = csvparser.parseLine(value.toString());
				
            if(validRecord(cols) && cols!=null && cols.length > 1){
            	//using the FlightCode and the month as key to reducer
                delay.set(cols[37]);
            	context.write(new FlightMonthKey(cols[6],Integer.parseInt(cols[2])), delay);
            	
            }           
        }
        /*
         * validRecord() is a method used for filtering out the flights
         * based on cancelled and checks if it matches 2008
         */
        
        public boolean validRecord(String record[]){
        	
        	//ignoring records which have any of the required fields empty
        	if(record[0].isEmpty()||record[2].isEmpty()||record[6].isEmpty()
        			||record[37].isEmpty() || record[41].isEmpty()){
        		return false;
        	}
        	
        	
        	//checking for year
        	if(!record[0].equals("2008")){
        		return false;
        	}
        	
        	//checking if its cancelled.
        	if(record[41].contains("1")){
        		return false;
        	}
        	
        	return true;
        	

    }
	}
	
	/*
	 * The reduce() gets the grouped Unique key (FlightCode) which contains delay for all the months.
	 * Extracts the delay from value and Month from Key, The average per month is calculated
	 * and written to the file.
	 */
    
    public static class Reduce extends Reducer<FlightMonthKey, Text, Text, Text> {
        
    	public void reduce(FlightMonthKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    		int curmonth = key.month;
    		double totaldelay = 0;
    		long avg = 0;
    		long totalflights = 0;
    		StringBuilder output = new StringBuilder();
    		for(Text del : values){
    			if (key.month!=curmonth){
    				avg = (long) Math.ceil(totaldelay/(float)totalflights);
    				output.append(", ("+curmonth+","+avg+")");
    				curmonth = key.month;
    				totaldelay = Double.parseDouble(del.toString());
    				totalflights = 1;
    			}else{
    				totaldelay+=Double.parseDouble(del.toString());
    				totalflights++;
    			}
    			
    		}
    		avg = (long) Math.ceil(totaldelay/(float)totalflights);
			output.append(", ("+curmonth+","+avg+")");
			context.write(new Text(key.flightCode), new Text(output.toString()));
    		
    		
        }
  
    }
    
    /*
	 * Custom partitioner to distribute the keys based on FlightCode to different reduce tasks.
	 * Source: Learning module on Blackboard under topic of Secondary sorting
	 */
    public static class FirstPartitioner
    extends Partitioner<FlightMonthKey, Text> {

    @Override
    public int getPartition(FlightMonthKey key, Text value, int numPartitions) {
      // multiply by 127 to perform some mixing
      return Math.abs(key.flightCode.hashCode() * 127) % numPartitions;
    }
  }
    
    /*
     * Key Comparator is used to ensure that two keys with same 
     * FlightCode are sorted by increasing order of Month.
     * 
     */
  
  public static class KeyComparator extends WritableComparator {
	  protected KeyComparator() {
			super(FlightMonthKey.class, true);
		}
    
    @SuppressWarnings("rawtypes")
	@Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      FlightMonthKey ip1 = (FlightMonthKey) w1;
      FlightMonthKey ip2 = (FlightMonthKey) w2;
      return ip1.compareTo(ip2);
  }
  }
  
  /*
   * Grouping comparator ensures that all keys with same 
   * FlightCode are grouped into one so that there is only
   * 1 reduce function call per flightcode.
   */
  
  public static class GroupComparator extends WritableComparator {
	  protected GroupComparator() {
			super(FlightMonthKey.class, true);
		}
    
    @SuppressWarnings("rawtypes")
	@Override
    public int compare(WritableComparable w1, WritableComparable w2) {
    	FlightMonthKey ip1 = (FlightMonthKey) w1;
        FlightMonthKey ip2 = (FlightMonthKey) w2;
      return ip1.flightCode.compareTo(ip2.flightCode);
    }
  }

  
    
    
    public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      
      org.apache.log4j.BasicConfigurator.configure();

      @SuppressWarnings("deprecation")
      Job job = new Job(conf, "Avg Flight Month delay");
      job.setJarByClass(MonthFlightDelay.class);
      job.setOutputKeyClass(FlightMonthKey.class);
      job.setOutputValueClass(Text.class);
      
      job.setNumReduceTasks(5);
//      job.setNumReduceTasks(10);

      job.setMapperClass(Map.class);
//    job.setCombinerClass(Reduce.class);
      job.setSortComparatorClass(KeyComparator.class);
      job.setGroupingComparatorClass(GroupComparator.class);
      job.setPartitionerClass(FirstPartitioner.class);
      job.setReducerClass(Reduce.class);
      

      job.setInputFormatClass(TextInputFormat.class);
      //This ensures files which are empty are not generated.
      LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class); 

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      

      job.waitForCompletion(true);
     
      
  }
   
}