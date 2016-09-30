import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

/**
 * 
 * @author M Krishna Kavya
 * 
 */
public class CommonFriend {

	/**
	 * The Map class extends the Mapper class.
	 */
	public static class Map extends
			Mapper<LongWritable, Text, LongWritable, Text> {
		/**
		 * The map method implements the logic of the Mapper function.
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
				LongWritable currentPerson = new LongWritable(
						Long.parseLong(friendsData[0]));
				/*
				 * fetching friends of a user.The friends indices are split with
				 * "," in the input file.
				 */
				String[] personFriends = friendsData[1].split(",");
				String one, two;
				// Initialising second person's key value pairs.

				LongWritable personTwoKey = new LongWritable();
				Text personTwoValue = new Text();
				/*
				 * 
				 * For every record in the input file. The currentPerson is
				 * initialized as a key. and all the friends associated to the
				 * key are set as value ( i.e,) index of friend appended with
				 * the tag "Friend".
				 */
				for (int i = 0; i < personFriends.length; i++) {
					one = personFriends[i];
					personOneValue.set(one + ",Friend");
					context.write(currentPerson, personOneValue);// key, value
																	// pairs of
																	// friends
																	// of
																	// current
																	// users.
					personOneKey.set(Integer.parseInt(one));
					personOneValue.set(one + ",Recommend");
					/*
					 * 
					 */
					for (int j = i + 1; j < personFriends.length; j++) {
						two = personFriends[j];
						personTwoKey.set(Integer.parseInt(two));
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
		public void reduce(LongWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			HashMap<String, Integer> hash = new HashMap<String, Integer>();
			String[] value;
			/*
			 * The look iterates through the values of a record.Each value is
			 * separated by "," if the value is equal to friend, The index of
			 * the person is added as the key and the value is -1.
			 */
			for (Text val : values) {
				value = val.toString().split(",");
				if (value[1].equals("Friend")) {
					System.out.println(value[0]);
					hash.put(value[0], -1);
					/*
					 * If the value is equal to Recommend and the Hash Map
					 * already contains the key, then the value is incremented
					 * by 1. else the value is added to be 1.
					 */
				} else if (value[1].equals("Recommend")) {
					if (hash.containsKey(value[0])) {
						if (hash.get(value[0]) != -1) {
							hash.put(value[0], hash.get(value[0] + 1));
						}
					} else {
						hash.put(value[0], 1);
					}
				}
			}

			// Initialization of array list
			ArrayList<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>();
			// The entries of the hashmap are added to the List.
			for (Entry<String, Integer> entry : hash.entrySet()) {
				if (entry.getValue() != -1) {
					list.add(entry);
				}
			}
			// The Array list is sorted based on the values using comparator.
			Collections.sort(list, new Comparator<Entry<String, Integer>>() {

				public int compare(Entry<String, Integer> o1,
						Entry<String, Integer> o2) {
					Integer one = o1.getValue();
					Integer two = o2.getValue();
					if (one > two) {
						return -1;
					} else if (one == two
							&& (Integer.parseInt(o1.getKey()) < Integer
									.parseInt(o2.getKey()))) {
						return -1;
					}
					return 1;
				}

			});
			/*
			 * Fetching top 10.
			 */
			int ten = 10;
			if (list.size() < 1) {
				context.write(key, new Text(StringUtils.join(",", list)));
			} else {
				ArrayList<String> top = new ArrayList<String>();
				for (int i = 0; i < Math.min(ten, list.size()); i++) {
					top.add(list.get(i).getKey());
				}
				context.write(key, new Text(StringUtils.join("", top)));
			}
		}

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
