import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 
 * @author M Krishna Kavya
 * 9/29/2016 1:12:28 PM
 * 
 */
public class CommonFriend {

	/**
	 * The Map class extends the Mapper class.
	 */
	public static class Map extends
			Mapper<LongWritable, Text, LongWritable, Text> {
		/**
		 * The map method implements the logic of the mapper function.
		 * 
		 * @throws InterruptedException
		 * @throws IOException
		 */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// Fetching one record from the input.
			String friendsIndex = value.toString();
			/*
			 * The input file of friends list has the number associated to one
			 * person and after a tab there are indices of the persons friend.
			 */
			String[] friendsData = friendsIndex.split("\t");

			/*
			 * Initializing key and value.
			 */
			LongWritable personOneKey = new LongWritable();
			Text personOneValue = new Text();
			// The friendsData consists of friend and the list of all his
			// friends.
			if (friendsData.length == 2) {
				// person- the user.
				LongWritable person = new LongWritable(
						Long.parseLong(friendsData[0]));
				/*
				 * fetching friends of a user.The friends indices are split with
				 * "," in the input file.
				 */
				String[] personFriends = friendsData[1].split(",");

				// Initialising second person's key value pairs.
				String one, two;
				LongWritable personTwoKey = new LongWritable();
				Text personTwoValue = new Text();
				// Processing all the friends list.
				for (int i = 0; i < personFriends.length; i++) {
					one = personFriends[i];
					personOneValue.set(one + ",Friend");
					context.write(person, personOneValue);
					personOneKey.set(Long.parseLong(one));
					personOneValue.set(one + ",Recommend");

					for (int j = i + 1; j < personFriends.length; j++) {
						two = personFriends[j];
						personTwoKey.set(Long.parseLong(two));
						personTwoValue.set(two + ",Recommend");
						context.write(personOneKey, personTwoValue);
						context.write(personTwoKey, personOneValue);
					}

				}

			}

		}
	}

	/**
	 * 
	 * The Reduce class extends the Reducer class.
	 * 
	 */
	public static class Reduce extends
			Reducer<LongWritable, Text, LongWritable, Text> {

	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		// Configuration.
		Configuration conf = new Configuration();
		String[] arguments = new GenericOptionsParser(conf, args)
				.getRemainingArgs();// Fetching arguments.

		/*
		 * Checking for all the arguments.
		 */
		if (arguments.length != 2) {
			System.err.println("Please give all the parameters.");
			System.exit(2);
		}

		// creating a job.
		Job job = new Job(conf, "CommonFriend");
		job.setJarByClass(CommonFriend.class);

		/*
		 * Setting the output key and value classes to Long and Text.
		 */
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		/*
		 * Setting Mapper and Reducer Class.
		 */
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		/*
		 * Adding Input and Output File Paths for the FileInputFormat and
		 * FileOutputFormat.
		 */
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
