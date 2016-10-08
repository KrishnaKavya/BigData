import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Question2 {

	static String friend1 = "";
	static String friend2 = "";
	static HashSet<Integer> set1 = new HashSet<Integer>();
	static HashSet<Integer> set2 = new HashSet<Integer>();

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		Long userOne = new Long(-1L);
		boolean first = false;

		/*
		 * The set up function takes the user id's of users in the parameters.
		 * They are set to static variables friend1, friend2.
		 */
		public void setup(Context context) {

			Configuration config = context.getConfiguration();
			friend1 = config.get("userA");
			friend2 = config.get("userB");
		}

		/*
		 * The Map method reads each line from the input file. and splits the
		 * user 1 and the list of friends. when the subject(user) is equal to
		 * one of the friend's ids give in the input. The data is added to a
		 * hashset. When the second user or friend is found, Check if the user
		 * id exists in the original hash. if yes, is added. else skipped.
		 */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split("\t");
			String subject = split[0];

			userOne = Long.parseLong(subject);

			if (split.length == 2) {
				String others = split[1];
				if ((subject.equals(friend1)) || (subject.equals(friend2))) {
					if (null != others) {
						String[] s = others.split(",");
						if (!first) {
							first = true;
							for (int i = 0; i < s.length; i++) {
								set1.add(Integer.parseInt(s[i]));
							}
						} else {
							for (int i = 0; i < s.length; i++) {
								if (set1.contains(Integer.parseInt(s[i]))) {
									set2.add(Integer.parseInt(s[i]));
								}
							}
						}
					}
				}
			}

		}
		/*
		 * The clean up writes the final output to the file. 
		 */
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			Text opKey = new Text(friend1 + "," + friend2);
			Text opVal = new Text(StringUtils.join(",", set2));
			context.write(opKey, opVal);
		}
	}

	public static void main(String args[]) throws Exception {
		// Standard Job setup procedure.
		Configuration conf = new Configuration();
		// Generic Options Parser fetches all the arguments.
		String[] arguments = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		System.out.println(Arrays.toString(arguments));
		//setting config variables from arguments. 
		conf.set("userA", arguments[0]);
		conf.set("userB", arguments[1]);
		//Setting a job name. 
		Job job = new Job(conf, "Question2");
		//Adding Mapper and reducer classes 
		job.setJarByClass(Question2.class);
		job.setMapperClass(Map.class);
		// setting reducer to 0. 
		job.setNumReduceTasks(0);
		//setting classes of output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(arguments[2]));

		FileOutputFormat.setOutputPath(job, new Path(arguments[3]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}