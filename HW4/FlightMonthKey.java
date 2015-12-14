package com.mapreduce.hw4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class FlightMonthKey implements WritableComparable{
	
	 String flightCode;
	 int month;
	 
	public FlightMonthKey(){
		
	}
	
	public FlightMonthKey(String name,int mon) {
		// TODO Auto-generated constructor stub
		this.flightCode = name;
		this.month = mon;
	}
	
	public void write(DataOutput output) throws IOException{
		output.writeUTF(this.flightCode);
		output.writeInt(this.month);
		
		
	}
	
	public void readFields(DataInput In) throws IOException{
		this.flightCode = In.readUTF();
		this.month = In.readInt();
	}
	public int compareTo(Object object){
		FlightMonthKey o = (FlightMonthKey)object;
		
		int val = this.flightCode.compareTo(o.flightCode);
		
		return val == 0 ? (this.month == o.month ? 0 : (this.month < o.month ? -1 : 1)) : val;
	}

}
