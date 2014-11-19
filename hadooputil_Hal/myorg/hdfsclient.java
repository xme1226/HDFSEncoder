//package myorg;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.StringTokenizer;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedWriter;
import java.io.FileWriter;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.mapred.UtilsForTests.RandomInputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//import myorg.hdfsutil;

public class hdfsclient {

	public hdfsclient() {

	}

  	public static class TokenizerMapper 
       		extends Mapper<Object, Text, Text, IntWritable>{
    
    		private final static IntWritable one = new IntWritable(1);
   		private Text word = new Text();
      
    		public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      			
			StringTokenizer itr = new StringTokenizer(value.toString());
			String tok,fName,retok,vadfs; 
      			while (itr.hasMoreTokens()) {
				tok = itr.nextToken();
				
				fName = tok.substring(0,tok.lastIndexOf("_"));	
				retok = tok.substring(0,tok.lastIndexOf("."))+".flv";	
				hdfsutil.getInstance().copyToLocal("/user/hadoop/data/"+fName+"/splitted/"+tok, "/home/hadoop");
        			vadfs = converter.getInstance().encodeFile("/home/hadoop/"+tok, "/home/hadoop/"+retok);
				hdfsutil.getInstance().copyFromLocal("/home/hadoop/"+retok, "/user/hadoop/data/"+fName+"/encoded/");
				File f = new File("/home/hadoop/"+tok);
				f.delete();
				f = new File("/home/hadoop/"+retok);
				f.delete();
			}
    		}
  	}
  
  	public static class IntSumReducer 
       		extends Reducer<Text,IntWritable,Text,IntWritable> {
    		private IntWritable result = new IntWritable();

    		public void reduce(Text key, Iterable<IntWritable> values, 
                	       Context context
                       	) throws IOException, InterruptedException {
      		/*int sum = 0;
      		for (IntWritable val : values) {
        	sum += val.get();
      		}
      	//	result.set(sum);
      	//	context.write(key, result);
    		
		hdfsutil.getInstance().copyToLocal("/movies/mylist.txt","/home/hadoop/movies");		
		String value = key.toString();
		for (int i=0;i<sum;i++){
			hdfsutil.getInstance().copyToLocal("/movies/_"+String.valueOf(i)+value, "/home/hadoop/movies");		
		}
		converter.getInstance().performMerge("/home/hadoop/movies/mylist.txt","/home/hadoop/movies/result.flv");*/
  	    }	
	}
	public static void main(String[] args) throws Exception {

	/*   	BufferedWriter output = new BufferedWriter(new FileWriter(file));
          	output.write(text);
          	output.close();
*/
	    	Configuration conf = new Configuration();
    		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    		/*if (otherArgs.length != 2) {
      		System.err.println("Usage: wordcount <in> <out>");
      		System.exit(2);
    		}*/
    		Job job = new Job(conf, "EncodeJob");
		job.setJarByClass(hdfsclient.class);
    		job.setMapperClass(TokenizerMapper.class);
    		job.setCombinerClass(IntSumReducer.class);
    		job.setReducerClass(IntSumReducer.class);
    		job.setOutputKeyClass(Text.class);
    		job.setOutputValueClass(IntWritable.class);
    		//job.setNumMapTasks(3);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    		System.exit(job.waitForCompletion(true) ? 0 : 1);

	/*	if (args[0].equals("copyfromlocal")) {
			if (args.length < 3) {
				System.out.println("Usage: hdfsclient copyfromlocal <from_local_path> <to_hdfs_path>");
				System.exit(1);
			}

			client.copyFromLocal(args[1], args[2]);
		}else if (args[0].equals("copytolocal")) {
			if (args.length < 3) {
				System.out.println("Usage: hdfsclient copytolocal <from_hdfs_path> <to_local_path>");
				System.exit(1);
			}

			client.copyToLocal(args[1], args[2]);
		}*/
	}
}
