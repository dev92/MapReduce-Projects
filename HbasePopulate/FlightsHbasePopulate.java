import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.opencsv.CSVParser;

public class FlightsHbasePopulate {
	
	public static class HbaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
		
		/*
		 * List of Column names which are used as qualifiers for Hbase Table
		 * with single column family 'flight'.
		 */
		
		List<String> qualifiers = Arrays.asList("Year","Quarter","Month","DayofMonth",
				"DayOfWeek","FlightDate","UniqueCarrier","AirlineID","Carrier",
				"TailNum","FlightNum","Origin","OriginCityName","OriginState",
				"OriginStateFips","OriginStateName","OriginWac","Dest","DestCityName",
				"DestState","DestStateFips","DestStateName","DestWac","CRSDepTime",
				"DepTime","DepDelay","DepDelayMinutes","DepDel15","DepartureDelayGroups",
				"DepTimeBlk","TaxiOut","WheelsOff","WheelsOn","TaxiIn","CRSArrTime",
				"ArrTime","ArrDelay","ArrDelayMinutes","ArrDel15","ArrivalDelayGroups",
				"ArrTimeBlk","Cancelled","CancellationCode","Diverted","CRSElapsedTime",
				"ActualElapsedTime","AirTime","Flights","Distance","DistanceGroup","CarrierDelay",
				"WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay"); 
		
		CSVParser csvparser = new CSVParser(',');
		HTable ht;
		List<Put> putList;
		
		/*
		 * Creates a client connection to hbase table for every Map Task,
		 * Since opening a connection to Hbase for every Map() is expensive,
		 * I open only once for very Map task. 
		 */
				
		
		protected void setup(Context context) throws IOException{
			org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
			ht = new HTable(config, "FlightData");
 			ht.setAutoFlush(false);   // makes it efficient for writing data
 			ht.setWriteBufferSize(102400); // write buffer for faster adding of data
 			putList=new ArrayList<Put>();
			
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String cols[] = csvparser.parseLine(value.toString());
            if(cols.length > 0){
            	// Row Key is CarrierID,FlightData,Offset key of file (To make a unique entry in hbase)
            	String mapkey = cols[6]+"@"+cols[5]+"@"+key.toString();
            	if(cols[41].isEmpty() || cols[4].matches("0.*")) cols[41] = "0";
            	
            	// Adding rowkey and columns correspondingly
            	
            	Put p = new Put(Bytes.toBytes(mapkey));
            	
            	for(int i=0;i<qualifiers.size();i++){
            		p.add("flight".getBytes(), qualifiers.get(i).getBytes(), cols[i].getBytes());
            		putList.add(p);            		
            	}
            	
           }
            
		}
		
		
		// Closing Table connection after a Map task
		protected void cleanup(Context context) throws IOException{
			ht.put(putList);
			ht.close();
		}
	}
	
	public static void main(String[] args) throws Throwable {
		
		Configuration conf = new Configuration();
	      
	      org.apache.log4j.BasicConfigurator.configure();

	      // Creating a Hbase table
	      createHTable();
	      
	      conf.set(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.OUTPUT_TABLE,"FlightData");
	 
	      Job job = new Job(conf, "H-Populate");
	      job.setJarByClass(FlightsHbasePopulate.class);
	      job.setOutputKeyClass(ImmutableBytesWritable.class);
	      job.setOutputValueClass(Put.class);
	      job.setMapperClass(HbaseMapper.class);
	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TableOutputFormat.class);
	      
	      // Since its a Map only job.
	      job.setNumReduceTasks(0);
	      
	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args[1]));
	      
	      
	      job.waitForCompletion(true);
	      
        
    }
	/*
	 * Creating Table named 'FlightData' and column family 'flight'
	 * Initially checks if table exists and deletes it accordingly
	 * and finally creates the table.
	 */
	
	public static void createHTable() throws IOException{
		Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor tableDescriptor = new HTableDescriptor("FlightData");
        tableDescriptor.addFamily(new HColumnDescriptor("flight"));
        boolean tableAvailable = admin.isTableAvailable("FlightData");
        
        if(tableAvailable){
        	admin.disableTable("FlightData");
        	admin.deleteTable("FlightData");
        }
       
        admin.createTable(tableDescriptor);
        admin.close();
	}

}
