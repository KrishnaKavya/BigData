import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 
 * @author M Krishna Kavya
 * 
 */
public class MyWordCount {

	/*
	 * Class that extends the Mapper Class. The methods of Map class take as the
	 * input and produce the intermediate key value pairs. The Generics of the
	 * Mapper class to specify what kind of input and output should be expected.
	 * In this class The first 2 arguments(LongWritable and Text(First pair)
	 * represent Input format and Text and InWritable(Second Pair) represents
	 * the output format.
	 */
	/*
	 * The Mapper for the word count gives the intermediate key value pairs of
	 * words. The mapper fpr every word in the input file makes a key value pair
	 * (word,1) That is The word acts and a key and its count is one.
	 */
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1); // Initialising
																	// final
																	// static
																	// variable
																	// with
																	// value 1
		private Text word = new Text();// word. output key

		/**
		 * The input file is first split into input split equal to the size of
		 * one block of HDFS. For each input split a mapper/Reducer is assigned.
		 * For Every Input Split there is an Record reader. The record Reader
		 * fetches one record at a time( one line) from the input split and
		 * gives to the map function.
		 * 
		 * LongWritable key- has the offset location of the record in the input
		 * split. Text value- has the record data.
		 * 
		 * @throws InterruptedException
		 * @throws IOException
		 */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] record = value.toString().split(" ");// The line of text is
															// split by spaces.
			/*
			 * for every word in the record. a Key value pair of ( word, 1) is
			 * written to context.
			 */
			for (String data : record) {
				word.set(data);
				context.write(word, one); // creates key value pairs.
			}
		}

	}

	/**
	 * The Reducer combines all the intermediate key value pairs into one. 
	 * Class that extends Reducer. The generics of the Reducer are also similar
	 * to mapper. Here The first pair represents the input that is the output of
	 * the mapper( Text(word), IntWritable(value(1)) and the second represents the output.
	 * (word,total occurrences in the input file.
	 * 
	 * The Reduce method gets all the values for a particular key. We compute the sum of all individual keys to form the total.
	 * @throws InterruptedException
	 * @throws IOException
	 * 
	 */
	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result=new IntWritable();
		public void reduce(Text Key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum=0;// initialize the sum for each keyword
			for(IntWritable value:values){
				sum+=value.get();
			}
			result.set(sum);
			context.write(Key, result);
		}
	}

	public static void main(String args[]) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		/*
		 * The Generic options parser is a utility to parse the command line
		 * arguments generic to the frame work. During the execution using
		 * Hadoop jar command. The input and output arguments are taken in the
		 * string array arguments.
		 */
		String[] arguments = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		/*
		 * The program is terminated if all the arguments are not received.
		 */
		if (arguments.length != 2) {
			System.err
					.println("Command to execute: hadoop jar <Addressofjar> <ClassName> <location of input> <location of output>");
			System.exit(2);
		}
		/*
		 * Job is a submitter's view of a job. It allows the user to configure
		 * the job, submit, control its execution and query its state. The set
		 * methods work only till the job is submitted. They throw an illegal
		 * argument exception.
		 */
		Job job = new Job(conf, "MyWordCount");

		// Adding class jar.
		job.setJarByClass(MyWordCount.class);

		// Adding Mapper and reducer classes
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		/*
		 * The expected output of the word count program is to get the key value
		 * pairs of words and its respective count in the input file. Hence, we
		 * set the outputKey class as Text and output value class as
		 * IntWritable(Integer).
		 */
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		/*
		 * The input and output files are present in HDFS. The arguments of
		 * execution gives the input/ output names. We set the input and output.
		 * The Map-Reduce framework relies on the InputFormat of the job to:
		 * 
		 * Validate the input-specification of the job. Split-up the input
		 * file(s) into logical InputSplits, each of which is then assigned to
		 * an individual Mapper. Provide the RecordReader implementation to be
		 * used to glean input records from the logical InputSplit for
		 * processing by the Mapper.
		 */
		FileInputFormat.addInputPath(job, new Path(arguments[0])); // input file
		FileOutputFormat.setOutputPath(job, new Path(arguments[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
