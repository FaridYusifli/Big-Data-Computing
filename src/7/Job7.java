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

/**
 *
 * @author bardh
 */
public class Job7 {
	private static MyRDFParser parser = new MyRDFParser();
     
	private static int printUsage() {
        	System.out.println("Job7: input_filename, output_filename");
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
           		job.setJarByClass(Job7.class);
            		job.setJobName("gervasi-prenkaj-Job7");
        	} catch (IOException ioex) {
            		Logger.getLogger(Job7.class.getName()).log(Level.SEVERE, "Couldn't set instance of mapreduce job with name Job7", ioex);
            		System.exit(-2);
        	}
        
       	 	try {
            		FileInputFormat.addInputPath(job, input);
            		FileOutputFormat.setOutputPath(job, output);
            		job.setMapperClass(Job7Mapper.class);
            		job.setReducerClass(Job7Reducer.class);

            		job.setInputFormatClass(TextInputFormat.class);

            		job.setOutputKeyClass(Text.class);
            		job.setOutputValueClass(Text.class);

			// ad external library jars for the NxParser
			addJarToClasspath(config, "/lib", job);

            		job.waitForCompletion(true);
        	} catch (IOException ioex) {
           		Logger.getLogger(Job7.class.getName()).log(Level.SEVERE, "Couldn't add input file " + input.getName() + ".", ioex);
            		System.exit(-2);
        	} catch (InterruptedException interrex) {
             		Logger.getLogger(Job7.class.getName()).log(Level.SEVERE, "Procedure interrupted.", interrex);
             		System.exit(-3);
         	} catch (ClassNotFoundException classnfex) {
             		Logger.getLogger(Job7.class.getName()).log(Level.SEVERE, "Couln't find one of the mapper/reducer classes.", classnfex);
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
	private static class Job7Mapper extends Mapper<LongWritable, Text, Text, Text> {
    		

    		@Override
    		protected void cleanup(Context context) throws IOException, InterruptedException { super.cleanup(context); }

    		@Override
    		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			HashMap<String, String> parsedLine = parser.parseLine(value.toString());
			//if(parsedLine.get("Context").equals("blankNode"))
			context.write(new Text(parsedLine.get("Subject")+" "+parsedLine.get("Predicate")+" "+parsedLine.get("Object")), new Text("OK"));	
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
	private static class Job7Reducer extends Reducer<Text, Text, Text, Text> {

    		@Override
    		protected void cleanup(Context context) throws IOException, InterruptedException { super.cleanup(context); }

    		@Override
    		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			context.write(key, new Text(""));
		} 

    		@Override
    		public void run(Context arg0) throws IOException, InterruptedException { super.run(arg0); }

    		@Override
    		protected void setup(Context context) throws IOException, InterruptedException { super.setup(context); }
	}
}
