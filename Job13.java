import java.io.IOException;
import java.io.File;
import java.io.FileNotFoundException;
import java.lang.InterruptedException;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * @author bardh
 */
public class Job13 {
	private static MyRDFParser parser = new MyRDFParser();
     
	private static int printUsage() {
        	System.out.println("Job13: input_filename, output_filename");
        	return -1;
    	} 
    
    	public static void main(String args[]) {
        	Configuration config = new Configuration();
        
        	if (args.length != 2) {
           	 System.out.println("ERROR: Wrong number of parameters: " + args.length + " instead of 2.");
           	 System.exit(printUsage());
        	}
        
       	 	Path input = new Path(args[0]);
        	Path output =new Path(args[1]);
        
        	Job job = null;
       	 	try {
            		job = Job.getInstance(config);
           		job.setJarByClass(Job13.class);
            		job.setJobName("gervasi-prenkaj-Job13");
        	} catch (IOException ioex) {
            		Logger.getLogger(Job13.class.getName()).log(Level.SEVERE, "Couldn't set instance of mapreduce job with name Job13", ioex);
            		System.exit(-2);
        	}
        
       	 	try {
            		FileInputFormat.addInputPath(job, input);
            		FileOutputFormat.setOutputPath(job, output);
            		job.setMapperClass(Job13Mapper.class);
            		job.setReducerClass(Job13Reducer.class);

            		job.setInputFormatClass(TextInputFormat.class);

            		job.setOutputKeyClass(Text.class);
            		job.setOutputValueClass(Text.class);

			// ad external library jars for the NxParser
			addJarToClasspath(config, "/lib", job);

            		job.waitForCompletion(true);
        	} catch (IOException ioex) {
           		Logger.getLogger(Job13.class.getName()).log(Level.SEVERE, "Couldn't add input file " + input.getName() + ".", ioex);
            		System.exit(-2);
        	} catch (InterruptedException interrex) {
             		Logger.getLogger(Job13.class.getName()).log(Level.SEVERE, "Procedure interrupted.", interrex);
             		System.exit(-3);
         	} catch (ClassNotFoundException classnfex) {
             		Logger.getLogger(Job13.class.getName()).log(Level.SEVERE, "Couln't find one of the mapper/reducer classes.", classnfex);
             		System.exit(-4);
         	}
    	}
	
	private static void addJarToClasspath(Configuration conf, String pathToLibrary, Job job) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(pathToLibrary), true);
		while(fileStatusListIterator.hasNext()) 
			job.addFileToClassPath(fileStatusListIterator.next().getPath());
	}

    
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	/**
 	*
 	* @author bardh
 	*/
	private static class Job13Mapper extends Mapper<LongWritable, Text, Text, Text> {
    		

    		@Override
    		protected void cleanup(Context context) throws IOException, InterruptedException { super.cleanup(context); }

    		@Override
    		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			HashMap<String, String> parsedLine = parser.parseLine(value.toString());
			for(String k : parsedLine.keySet()) {
				if(k.equals("Subject")) {
					context.write(new Text("node-"+parsedLine.get(k)), new Text("OK"));
				} else if(k.equals("Object")) {
					context.write(new Text("node-"+parsedLine.get(k)), new Text("OK"));
					context.write(new Text(parsedLine.get(k)), new Text("1"));		
				} else if(k.equals("Predicate"))
					context.write(new Text("edge-"+parsedLine.get(k)), new Text("OK"));
			}

    		}

    		@Override
    		public void run(Context context) throws IOException, InterruptedException { super.run(context); }

    		@Override
    		protected void setup(Context context) throws IOException, InterruptedException { super.setup(context); }

	}
//////////////////////////////////////////////////////////////////////////////////////////////////////

	/*
	  *
	  * @author bardh
	*/
	private static class Job13Reducer extends Reducer<Text, Text, Text, IntWritable> {

		private HashSet<String> nodes;
		private HashSet<String> edges;

    		@Override
    		protected void cleanup(Context context) throws IOException, InterruptedException { 
			super.cleanup(context); 
			context.write(new Text("Number of distinct nodes is:"), new IntWritable(nodes.size()));
			context.write(new Text("Number of distinct edges is:"), new IntWritable(edges.size()));
		}

    		@Override
    		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if(key.toString().startsWith("node-") && !nodes.contains(key.toString())) nodes.add(key.toString());
			else if(key.toString().startsWith("edge-") && !edges.contains(key.toString())) edges.add(key.toString());
			else {	
				int indegree = 0;
				for(Text val : values)
					indegree += Integer.parseInt(val.toString());
				context.write(key, new IntWritable(indegree));
      			}	
		} 

    		@Override
    		public void run(Context arg0) throws IOException, InterruptedException { super.run(arg0); }

    		@Override
    		protected void setup(Context context) throws IOException, InterruptedException{ 
			super.setup(context); 
			nodes = new HashSet<String>();
			edges = new HashSet<String>();
		}
	}
}
