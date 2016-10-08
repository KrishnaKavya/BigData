import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question1 {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		/*
		 * The map method is reads file and adds key value pairs of one friends
		 * to all the friend in the friends list.
		 */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] line = value.toString().split("\t");
			if (line.length > 1) {
				String p = line[0];
				String list = line[1];
				String[] friends = list.split(",");
				for (String friend : friends) {
					String redkey = buildSortedKey(p, friend);
					Text values = new Text(line[1]);
					Text keys = new Text(redkey);

					context.write(keys, values);
				}

			}

		}
		/*
		 * Method to sort the key value pairs.
		 */
		public String buildSortedKey(String person1, String friend) {
			Integer pno = Integer.parseInt(person1);
			Integer fno = Integer.parseInt(friend);
			String key = null;
			if (pno > fno) {
				key = pno.toString() + " , " + fno.toString();
			}

			else
				key = fno.toString() + " , " + pno.toString();

			// TODO Auto-generated method stub
			return key;
		}
	}
	/*
	 * Fetching the sorted pairs from the mapper and forms a mutual friends list. 
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		// private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashMap friendnum = new HashMap<String, Integer>();
			String mutualfriend = " 	<";
			Boolean hasfriend = false;
			for (Text val : values) {

				String[] friends = val.toString().split(",");

				for (String a : friends) {
					if (friendnum.containsKey(a)) {
						friendnum.put(a, 2);
					} else {
						friendnum.put(a, 0);

					}
				}
			}
			Set<String> keySet = friendnum.keySet();
			Iterator<String> keySetIterator = keySet.iterator();
			while (keySetIterator.hasNext()) {

				String k = keySetIterator.next();
				Integer common = (Integer) friendnum.get(k);
				if (common == 2) {
					if (hasfriend) {
						mutualfriend = mutualfriend + "," + k;
					} else {
						hasfriend = true;
						mutualfriend = mutualfriend + "," + k;
					}
				}
			}
			mutualfriend += ">\n";

			if (!hasfriend)
				mutualfriend = "<no mutual friend>";
			Text friendlist = new Text(mutualfriend);
			context.write(key, friendlist);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: MutualFriend <in> <out>");
			System.exit(2);
		}
		// create a job with name "wordcount"
		Job job = new Job(conf, "mutualfriend");
		job.setJarByClass(Question1.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		// uncomment the following line to add the Combiner
		// job.setCombinerClass(Reduce.class);
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
