package com.hbase.compute.hw4;

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
	//Constructor for the class which takes flightCode and Month
	public FlightMonthKey(String name,int mon) {
		// TODO Auto-generated constructor stub
		this.flightCode = name;
		this.month = mon;
	}
	
	// Required by WritableComparable Interface
	public void write(DataOutput output) throws IOException{
		output.writeUTF(this.flightCode);
		output.writeInt(this.month);
		
		
	}
	// Required by WritableComparable Interface
	public void readFields(DataInput In) throws IOException{
		this.flightCode = In.readUTF();
		this.month = In.readInt();
	}
	
	// Actual logic to perform Secondary Sorting
	public int compareTo(Object object){
		FlightMonthKey o = (FlightMonthKey)object;
		
		int val = this.flightCode.compareTo(o.flightCode);
		
		return val == 0 ? (this.month == o.month ? 0 : (this.month < o.month ? -1 : 1)) : val;
	}

}
