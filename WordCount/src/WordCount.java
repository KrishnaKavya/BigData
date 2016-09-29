import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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

public class WordCount {
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
		private final static IntWritable one = new IntWritable(1);//Initialising final static variable
		private Text word = new Text(); // type of output key
		
				
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
			String[] mydata = value.toString().split(" ");// The line of text is
								     // split by spaces.
			/*
			 * for every word in the record. a Key value pair of ( word, 1) is
			 * written to context.
			 */
			for (String data : mydata) {
				word.set(data); // set word as each input keyword
				context.write(word, one); // create a pair <keyword, 1>
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
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0; // initialize the sum for each keyword
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result); // create a pair <keyword, number of
										// occurences>
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: WordCount <in> <out>");
			System.exit(2);
		}
		// create a job with name "wordcount"
		Job job = new Job(conf, "wordcount");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		// uncomment the following line to add the Combiner
		// job.setCombinerClass(Reduce.class);
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(IntWritable.class);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
