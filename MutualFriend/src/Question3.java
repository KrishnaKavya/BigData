import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Question3 extends Configured implements Tool {
	static String t = "";
	static HashMap<String, String> map;
	static HashSet<Integer> temp = new HashSet<Integer>();

	public static class FriendData extends Mapper<Text, Text, Text, Text> {
		// A String Builder is initialized to [.
		StringBuilder friendData = new StringBuilder("[");
		String keyUser = "";

		/*
		 * Setting data path, fetching the file from the path and opening the
		 * file.
		 */
		public void setup(Context context) throws IOException {
			Configuration config = context.getConfiguration();
			map = new HashMap<String, String>();
			String mybusinessdataPath = config.get("businessdata");
			Path pt = new Path("hdfs://cshadoop1" + mybusinessdataPath);
			FileSystem fs = FileSystem.get(config);
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(pt)));
			String line;
			line = br.readLine();
			/*
			 * till the end of file, adding the adding name and date of birth of
			 * the person.
			 */
			while (line != null) {
				String[] arr = line.split(",");
				if (arr.length == 10) {
					String data = arr[1] + ":" + arr[9];
					map.put(arr[0].trim(), data);
				}
				line = br.readLine();
			}
		}

		int count = 0;

		/*
		 * The mapper method reads the line splits it based on, if the key is
		 * available its appended to the string builder.
		 */
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split(",");
			keyUser = key.toString();
			if (null != map && !map.isEmpty()) {
				for (String s : split) {
					if (map.containsKey(s)) {
						friendData = friendData.append(map.get(s) + ",");
						map.remove(s);
					}
				}
			}
		}
		/*
		 * setting the length of the string builder and ending with ].
		 * adding and writing key value pairs
		 * 
		 */
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			friendData.setLength(friendData.length() - 1);
			friendData.append("]");
			Text opVal = new Text(friendData.toString());
			Text opKey = new Text(keyUser);
			context.write(opKey, opVal);
		}
	}

	public static void main(String args[]) throws Exception {
		// Standard Job setup procedure.
		int res = ToolRunner.run(new Configuration(), new Question3(), args);
		System.exit(res);

	}
	/*
	 * The run method initialises and calls all the methods. 
	 */
	public int run(String[] otherArgs) throws Exception {
		Configuration conf = new Configuration();
		if (otherArgs.length != 6) {
			System.err
					.println("Usage: InMemoryJoin <user1> <user2> <input> <out> <inmemory input> <output>");
			System.exit(2);
		}
		//fetching user id's from input.
		conf.set("userA", otherArgs[0]);
		conf.set("userB", otherArgs[1]);
		//Initialising and adding a job.
		Job job = new Job(conf, "Question3");
		job.setJarByClass(Question3.class);
		//Setting the mapper and output key, value classes. 
		job.setMapperClass(Question2.Map.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//setting output from the input. 
		FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
		Path p = new Path(otherArgs[3]);
		FileOutputFormat.setOutputPath(job, p);
		int code = job.waitForCompletion(true) ? 0 : 1;
		Configuration conf1 = getConf();
		conf1.set("businessdata", otherArgs[4]);
		Job job2 = new Job(conf1, "Question3");
		job2.setJarByClass(Question3.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setMapperClass(FriendData.class);
		job2.setNumReduceTasks(0);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, p);
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[5]));
		code = job2.waitForCompletion(true) ? 0 : 1;
		System.exit(code);
		return code;
	}
}