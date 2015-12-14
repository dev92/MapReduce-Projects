package com.hbase.compute.hw4;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class FlightsHbaseCompute {
	
	public static class HbaseMapper extends TableMapper<FlightMonthKey, Text>{
		
		Text delay = new Text();
		
		public void map(ImmutableBytesWritable rowkey, Result value, Context context) throws IOException, InterruptedException {
            
			// Extracting the Flightcode and Month from date.
			String row[] = new String(rowkey.get()).split("@");
            SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");
            java.util.Date flightdate = null;
			try {
				flightdate = date.parse(row[1]);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			// retrieving the Delay qualifier and passing it to next stage.
			String Delaymins = new String(value.getValue("flight".getBytes(), "ArrDelayMinutes".getBytes())); 
          
            if(!Delaymins.isEmpty()){
            	@SuppressWarnings("deprecation")
				FlightMonthKey mapkey = new FlightMonthKey(row[0], flightdate.getMonth()+1);
            	delay.set(Delaymins);
            	context.write(mapkey, delay);
            }
            
            	
	}
}
	
public static class Reduce extends Reducer<FlightMonthKey, Text, Text, Text> {
        
	/*
	 * The reduce() gets the grouped Unique key (FlightCode) which contains delay for all the months.
	 * Extracts the delay from value and Month from Key, The average per month is calculated
	 * and written to the file.
	 * 
	 */
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
	 */
    public static class FirstPartitioner extends Partitioner<FlightMonthKey, Text> {

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
      // compareTo() is detailed in FlightMonthKey class
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

	
	public static void main(String[] args) throws Throwable {
		Configuration conf = new Configuration();

	      
	      org.apache.log4j.BasicConfigurator.configure();
	  
	      Job job = new Job(conf, "H-Compute");
	      job.setJarByClass(FlightsHbaseCompute.class);
	      job.setOutputKeyClass(FlightMonthKey.class);
	      job.setOutputValueClass(Text.class);
	      job.setMapperClass(HbaseMapper.class);
	      job.setSortComparatorClass(KeyComparator.class);
	      job.setGroupingComparatorClass(GroupComparator.class);
	      job.setPartitionerClass(FirstPartitioner.class);
	      job.setReducerClass(Reduce.class);
	      
	      /*
	       * Adding filters to filter out Columns with year
	       * matching 2008 and Non-cancelled flights
	       */
	      
	      FilterList filters = new FilterList(Operator.MUST_PASS_ALL);
	      SingleColumnValueFilter year = new SingleColumnValueFilter("flight".getBytes(), 
	    		  "Year".getBytes(), CompareOp.EQUAL, "2008".getBytes());
	      SingleColumnValueFilter cancelled = new SingleColumnValueFilter("flight".getBytes(), 
	    		  "Cancelled".getBytes(), CompareOp.EQUAL, "0".getBytes());
	      filters.addFilter(year);
	      filters.addFilter(cancelled);
	      
	      Scan sc = new Scan();
	      sc.setCacheBlocks(false);  //required for MR jobs
	      sc.setCaching(500);
	      sc.setFilter(filters);
	      
	      /*
	       *  Method specifying Hbase table, Scan to be applied and Mapper Class to process
	       *  the table rows and OutputKey class and Outputvalue class for the job.
	       */
	      org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.initTableMapperJob("FlightData", 
	    		  sc, HbaseMapper.class, FlightMonthKey.class, Text.class, job);
	    		 
	      
	      
//	      job.setNumReduceTasks(5);
	      job.setNumReduceTasks(10);
	     
	      // To ensure empty files are created
	      LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
	      FileOutputFormat.setOutputPath(job, new Path(args[0]));
	      
	      
	      job.waitForCompletion(true);
	       
        
    }
	
	
}
