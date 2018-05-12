package com.homework3.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.homework3.pagerank.Adjacency.ParserInitialPgMapper;
import com.homework3.pagerank.Adjacency.ParserMapper;
import com.homework3.pagerank.Adjacency.ParserReducer;
import com.homework3.pagerank.Pagerank.Node;
import com.homework3.pagerank.Pagerank.DeltaMapper;
import com.homework3.pagerank.Pagerank.RankReducer;
import com.homework3.pagerank.Pagerank.TokenizerMapper;
import com.homework3.pagerank.TopK.TopKMapper;
import com.homework3.pagerank.TopK.TopKReducer;

public class PageRankDriver {

	public static void main(String[] args) throws Exception {
		
        Configuration conf = new Configuration();
        conf.setDouble("alpha", 0.15);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: pagerank <in> [<out>...] <out>");
            System.exit(2);
        }
        
        // Preprocess the WIKI dump and create a graph with adjacency list
        Job PreProcessjob = Job.getInstance(conf, "preprocess");
        PreProcessjob.setJarByClass(Adjacency.class);
        PreProcessjob.setMapperClass(ParserMapper.class);
        PreProcessjob.setReducerClass(ParserReducer.class);
        PreProcessjob.setOutputKeyClass(Text.class);
        PreProcessjob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(PreProcessjob, new Path(otherArgs[0])); 
        FileOutputFormat.setOutputPath(PreProcessjob,new Path(otherArgs[otherArgs.length - 1]+"/Graph"));
        PreProcessjob.waitForCompletion(true);
        
        // Map-only job to initialize the pagerank values for the graph
        Job Initialjob = Job.getInstance(conf, "InitialPagerank");
        Initialjob.getConfiguration().setDouble("Pages", PreProcessjob.getCounters().findCounter(PageCount.Counter).getValue());
        Initialjob.setJarByClass(Adjacency.class);
        Initialjob.setMapperClass(ParserInitialPgMapper.class);
        Initialjob.setOutputKeyClass(Text.class);
        Initialjob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(Initialjob, new Path(otherArgs[otherArgs.length - 1]+"/Graph"));
        FileOutputFormat.setOutputPath(Initialjob,new Path(otherArgs[otherArgs.length - 1]+"/InitialGraph"));
        Initialjob.waitForCompletion(true);

        // First Page rank iteration
        Job job = Job.getInstance(conf, "pagerank");
        job.getConfiguration().setDouble("delta", PreProcessjob.getCounters().findCounter(DeltaCounter.Counter).getValue());
        job.getConfiguration().setDouble("Pages", PreProcessjob.getCounters().findCounter(PageCount.Counter).getValue());
        job.setJarByClass(Pagerank.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(RankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Node.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[otherArgs.length - 1]+"/InitialGraph"));
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]+"/Output1"));
        job.waitForCompletion(true);
        
        String input=otherArgs[otherArgs.length - 1]+"/Output1";
        String output=otherArgs[otherArgs.length - 1]+"/Output2";
       
        double del = job.getCounters().findCounter(DeltaCounter.Counter).getValue()/Math.pow(10, 10);
        
        // Run through the remaining 9 iterations
        for (int i =3 ; i<12; i++) {
        	Job Nextjob = Job.getInstance(conf, "pagerank");
         	Nextjob.getConfiguration().setDouble("delta",  del);
         	Nextjob.getConfiguration().setDouble("Pages", PreProcessjob.getCounters().findCounter(PageCount.Counter).getValue());
        	Nextjob.setJarByClass(Pagerank.class);
        	Nextjob.setMapperClass(TokenizerMapper.class);
        	Nextjob.setReducerClass(RankReducer.class);
        	Nextjob.setOutputKeyClass(Text.class);
        	Nextjob.setOutputValueClass(Node.class);
            FileInputFormat.addInputPath(Nextjob, new Path(input));
            FileOutputFormat.setOutputPath(Nextjob,new Path(output));
            
            input = output;
            output= otherArgs[otherArgs.length - 1]+"/"+"Output"+i;
            Nextjob.waitForCompletion(true);
            del = Nextjob.getCounters().findCounter(DeltaCounter.Counter).getValue();
            del = del / Math.pow(10, 10);
            
        }
        
        input = otherArgs[otherArgs.length - 1]+"/"+"Output10";
        output = otherArgs[otherArgs.length - 1]+"/"+"OutputFinal";
        
        // Final map job to distribute delta
        Job finaljob = Job.getInstance(conf, "pagerank");
        finaljob.getConfiguration().setDouble("delta",  del);
        finaljob.getConfiguration().setDouble("Pages", PreProcessjob.getCounters().findCounter(PageCount.Counter).getValue());
        finaljob.setJarByClass(Pagerank.class);
        finaljob.setMapperClass(DeltaMapper.class);
        finaljob.setOutputKeyClass(Text.class);
        finaljob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(finaljob, new Path(input));
        FileOutputFormat.setOutputPath(finaljob,new Path(output));
        finaljob.waitForCompletion(true);
       
        input = otherArgs[otherArgs.length - 1]+"/"+"OutputFinal";
        String finalOutput=otherArgs[otherArgs.length - 1]+"/"+"Top100";
        
        
        // Map-reduce to retrieve top 100 pages
        Job TopJob = Job.getInstance(conf, "topk");
        TopJob.setJarByClass(TopK.class);
        TopJob.setMapperClass(TopKMapper.class);
        TopJob.setReducerClass(TopKReducer.class);
        TopJob.setOutputKeyClass(DoubleWritable.class);
        TopJob.setNumReduceTasks(1);
        TopJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(TopJob, new Path(input));
        
        FileOutputFormat.setOutputPath(TopJob,new Path(finalOutput));
        TopJob.waitForCompletion(true);
        
        System.exit(0);
        
        
	}
	
	public enum DeltaCounter{
		Counter
	}
	
	public enum PageCount {
		Counter
	}
}


