package com.homework3.pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TopK {

	
	// Mapper class to retrieve Top 100 pages with highest pagerank (As per Professor slides to compute Top K)
	public static class TopKMapper extends Mapper<Object, Text, DoubleWritable, Text>{
	
		private TreeMap<Double, String> repToRecordMap = new TreeMap<Double, String>();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			List<String> tokens = new ArrayList<String>();
			tokens = Arrays.asList(value.toString().split("\\s+"));
			
			if (tokens.get(0) == null || tokens.get(1) == null) {
				return;
			}
			repToRecordMap.put(Double.parseDouble(tokens.get(1)),tokens.get(0));

			if (repToRecordMap.size() > 100) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}
	
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Double t : repToRecordMap.keySet()) {
				context.write(new DoubleWritable(t),new Text(repToRecordMap.get(t)));
			}
		  }
		}
	
	
	// Reducer class to retrieve Top 100 pages (As per Professor slide to compute Top K)
	public static class TopKReducer extends Reducer<DoubleWritable,Text,Text,DoubleWritable> {
		
		private TreeMap<Double, Text> repToRecordMap = new TreeMap<Double, Text>();
		
		public void reduce(DoubleWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				repToRecordMap.put(new Double(key.get()),new Text(value));
				if (repToRecordMap.size() > 100) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}	
		}
		
		protected void cleanup(Context context)throws IOException, InterruptedException{
			for (Double t : repToRecordMap.descendingMap().keySet()) {
				context.write(new Text(repToRecordMap.get(t)),new DoubleWritable(t));
			}
		}

	}
}
