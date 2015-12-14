package com.mapreduce.hw3;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


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


 
public class AvgFlightDelay {
	
	/*
	 * FLIGHT_COUNTER is an enum required to give names for the hadoop
	 * global counter 
	 * FlightDelay counter is for storing total Delay value.
	 * TotalFlights is counter for number of flights satisfying the condition 
	 */
	
	public enum FLIGHT_COUNTER {
		  FlightDelay,
		  TotalFlights
	}
	
 
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        
		private Text maprecord = new Text();
        private Text mapKey = new Text();
        CSVParser csvparser = new CSVParser(',');
        Date startDate,endDate,flight;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

                
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String cols[] = csvparser.parseLine(value.toString());
            StringBuilder str = new StringBuilder();
				
            if(validRecord(cols) && cols!=null && cols.length > 1){
            	//using the intermediate stop and the flight date as key to reducer
            	if(cols[11].toLowerCase().equals("ord")){
            		mapKey.set(cols[5]+"-"+cols[17]);
            		str.append(cols[11].toLowerCase()+",");
            	}else {
            		mapKey.set(cols[5]+"-"+cols[11]);
            		str.append(cols[17].toLowerCase()+",");
            	}
            	str.append(cols[24]+","+cols[35]+","+cols[37]);
            	// removing uneccesary fields and using the record as value to reducer
            	maprecord.set(str.toString());
            	
            	context.write(mapKey, maprecord);
            }

            
        } 
        /*
         * validRecord() is a method used for filtering out the flights
         * based on cancelled/diverted, if origin is 'ord' or destination
         * is 'jfk' and finally checks if it is within specified date range.
         */
        
        public boolean validRecord(String record[]){
        	
        	//ignoring records which have any of the required fields empty
        	if(record[5].isEmpty()||record[11].isEmpty()||record[17].isEmpty()
        			||record[24].isEmpty()||record[35].isEmpty()||record[37].isEmpty()
        			||record[41].isEmpty()||record[43].isEmpty()){
        		return false;
        	}
        	
        	try {
				startDate = dateFormat.parse("2007-06-01");
				endDate = dateFormat.parse("2008-05-31");
	        	flight = dateFormat.parse(record[5]);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
        	
        	//checking within date range
        	if(flight.before(startDate) || flight.after(endDate)){
        		return false;
        	}
        	
        	//checking if its cancelled or diverted.
        	if(record[41].contains("1") || record[43].contains("1")){
        		return false;
        	}
        	
        	// Condition to ensure it is not a single-legged flight
        	if(record[11].toLowerCase().equals("ord") && record[17].toLowerCase().equals("jfk")){
        		return false;
        	}
        	
        	if(record[11].toLowerCase().equals("ord") || record[17].toLowerCase().equals("jfk")){
        		return true;
        	}else{
        		return false;
        	}
        	
        }

    }
	
	
    
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        
    	private long totalflights,totaldelay;
    	
    	// The reason for using setup() is to initialize values for each reducer task
    	public void setup(){
    		this.totalflights = 0;
    		this.totaldelay = 0;
    	}
    	/*
    	 * We create two lists for each type of record, i.e., one list for 'ord' flights
    	 * and other one for 'jfk' flights.
    	 */
    	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    		ArrayList<String> ordFlights = new ArrayList<String>();
    		ArrayList<String> jfkFlights = new ArrayList<String>();
    		for(Text rec : values){
    			String line[] = rec.toString().split(",");
    			if(line[0].equals("ord")){
    				ordFlights.add(rec.toString());
    			}else{
    				jfkFlights.add(rec.toString());
    			}
    		}
    		for(String ordflight : ordFlights){
    			String ordetails[] = ordflight.split(",");
    			for(String jfkflight : jfkFlights){
    				String jfketails[] = jfkflight.split(",");
						if(twoLegged(ordetails,jfketails)){
							this.totalflights++;
							this.totaldelay+=(Float.parseFloat(ordetails[3])
									          +Float.parseFloat(jfketails[3]));
						}
    			}
    		}
        }
    	/* cleanup() is called per reducer task, hence its prime purpose to increment counter
    	 * after each reduce task.
    	 */
    	public void cleanup(Context context) throws IOException, InterruptedException{
    		context.getCounter(FLIGHT_COUNTER.TotalFlights).increment(this.totalflights);
    		context.getCounter(FLIGHT_COUNTER.FlightDelay).increment(this.totaldelay);
    		
    	}
    	/*
    	 * twoLegged() method checks if the 'ord' flight arrival time is less than
    	 * 'jfk' flight departure time, to ensure its a valid two legged flight.
    	 */
    	public boolean twoLegged(String ord[],String jfk[]){
    		
    			if(Integer.parseInt(ord[2]) < Integer.parseInt(jfk[1])){
    				return true;
    			}else{
    				return false;
    			}
    	}
    }
  
    
    
    public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      
      org.apache.log4j.BasicConfigurator.configure();

      @SuppressWarnings("deprecation")
      Job job = new Job(conf, "Avg Flight delay");
      job.setJarByClass(AvgFlightDelay.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      
      job.setNumReduceTasks(10);

      job.setMapperClass(Map.class);
      //job.setCombinerClass(Reduce.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      //Since no output is written into files, this ensures files are not generated.
      LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class); 

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      

      job.waitForCompletion(true);
      
      // To display final Counter values and calculated Total Delay Average
      Counters counter = job.getCounters();
      Counter c1 = counter.findCounter(FLIGHT_COUNTER.FlightDelay);
      Counter c2 = counter.findCounter(FLIGHT_COUNTER.TotalFlights);
      System.out.println("Totat"+c1.getDisplayName()+"(mins): "+c1.getValue());
      System.out.println(c2.getDisplayName()+":"+c2.getValue());
      System.out.println("Average Flight Delay (mins)"+":"+c1.getValue()/(float)c2.getValue());
  }
   
}