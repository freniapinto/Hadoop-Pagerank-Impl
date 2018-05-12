package com.homework3.pagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.homework3.pagerank.PageRankDriver.DeltaCounter;

public class Pagerank {
	
	// Custom class Node which stores a page, page rank and its adjacency list
	public static class Node implements Writable {
		private Text name;
		private DoubleWritable rank;
		private Text adj;
		private DoubleWritable otherRank;
		private Text flag;
		
		public Node() {	
			name = new Text();
			rank = new DoubleWritable();
			otherRank = new DoubleWritable();
			adj = new Text();
			flag = new Text();
		}
		
		public Node(DoubleWritable w) {
			this.flag = new Text("False");
			this.otherRank = w;
		}
		
		public Node(Text n, DoubleWritable r, Text a) {
			this.flag = new Text("True");
			this.name =n;
			this.rank =r;
			this.adj =a;
		}
	
		public Text getNodeName() {
			return name;
		}
		
		public DoubleWritable getRank() {
			return rank;
		}

		public Text getAdjList() {
			return adj;
		}
		
		public DoubleWritable getOtherRank() {
			return otherRank;
		}
		
		public Text getNodeType() {
			return flag;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			flag.write(out);
			if (flag.toString().equals("True")) {
				name.write(out);
				rank.write(out);
				if(adj != null) {
					(new Text("False")).write(out);
					adj.write(out);
				}
				else {
					(new Text("True")).write(out);
				}
					
			}
			else otherRank.write(out);			
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			flag.readFields(in);
			Text adjNullFlag = new Text();
			if (flag.toString().equals("True")) {
				name.readFields(in);
				rank.readFields(in);
				adjNullFlag.readFields(in);
				
				if(adjNullFlag.toString().equals("False")) {
					adj.readFields(in);
				}
				else
					adj= new Text();
			}
			else otherRank.readFields(in);
		}
		
		
		public void setPagerank(double r) {
			rank = new DoubleWritable(r);
		}
	}
	
	
	// Mapper class as per Professor slides V2
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Node> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			List<String> tokens = new ArrayList<String>();
			Configuration conf = context.getConfiguration();
			double alpha =conf.getDouble("alpha",0.0);
			double pageNum = conf.getDouble("Pages", 0.0);
			double delta = conf.getDouble("delta", 0.0);
			Text adjList;
			tokens = Arrays.asList(value.toString().split("\\s+"));
			if (tokens.size() <=2) {
				adjList=null;
				}
			else {
				adjList = new Text(tokens.get(2));
			}
			
			Node n = new Node(new Text(tokens.get(0)),new DoubleWritable(Double.parseDouble(tokens.get(1))), adjList);
			double newPgr = n.getRank().get() + ((1-alpha)*delta)/pageNum;
			n.setPagerank(newPgr);
			context.write(n.getNodeName(), n);
			
			if(n.getAdjList()!=null) {
				String[] list= n.getAdjList().toString().split("~");
				int count = list.length;
				Double contributions =Double.parseDouble(n.getRank().toString())/(double) count;
				for (String each: list) {
					Node newNode = new Node(new DoubleWritable(contributions));
					context.write(new Text(each), newNode);
				}
			}
			else {
				long rank = (long) (Math.pow(10, 10) * n.getRank().get());
				context.getCounter(DeltaCounter.Counter).increment(rank);
			}
		}
	}
	
	
	// Mapper to distribute the final delta values to all pagerank
	public static class DeltaMapper extends Mapper<Object, Text, Text, Text> {
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			List<String> tokens = new ArrayList<String>();
			Configuration conf = context.getConfiguration();
			double alpha =conf.getDouble("alpha",0.0);
			double pageNum = conf.getDouble("Pages", 0.0);
			double delta = conf.getDouble("delta", 0.0);
			Text adjList;
			tokens = Arrays.asList(value.toString().split("\\s+"));
			double rank = Double.parseDouble(tokens.get(1));
			double newPgr = rank + ((1-alpha)*delta)/pageNum;
			if (tokens.size() <=2) {
				context.write(new Text(tokens.get(0)), new Text(Double.toString(newPgr)));
				}
			else {
				adjList = new Text(tokens.get(2));
				context.write(new Text(tokens.get(0)), new Text(Double.toString(newPgr)+" "+adjList));
			}
			
			
			
		}
	}
	
	// Reducer task as per Professor slides V2
	public static class RankReducer extends Reducer<Text, Node, Text, Text> {
		
		private Node n = new Node();
		
		public void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
			double S=0.0;
			Configuration conf = context.getConfiguration();
			for (Node value : values) {
				if(value.getNodeType().toString().equals("True")) {
					n =value;
				}
				else {
					S+= value.getOtherRank().get();
				}
			}
			double alpha =conf.getDouble("alpha",0.0); 
			double numPages = conf.getDouble("Pages", 0.0);
			double newPagerank = (alpha/numPages) + ((1-alpha)*S);
			n.setPagerank(newPagerank);
			Text t = n.getAdjList();

			context.write(key, new Text(n.getRank()+ " "+ t));
		}
	}	
}
