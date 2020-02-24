package app.hadoop;

/*Q1 Write a MapReduce program in Hadoop that implements a simple “Mutual/Common friend list of two friends". 
 * 		The key idea is that if two people are friend then they have a lot of mutual/common friends. 
 * 		This program will find the common/mutual friend list for them.
 *
 *Input: soc-LiveJournal1Adj.txt contains the adjacency list and the userdata.txt contains dummy data
 *
 *Output: The output should contain one line per user in the following format:
 *		<User_A>, <User_B><TAB><Mutual/Common Friend List>
 * */

/**
 * @author amtulnazneen
 *
 */

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

import utils.Constants;
import utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class Question1 {

	public static class Q1Map1 extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {

			String[] line = values.toString().split(Constants.TAB);
			if (line.length == 2)
			{
				int userId = Utils.toInt(line[0]);
				String[] friendsList = line[1].split(Constants.COMMA);
				Text pair = new Text();
				for (String tempUser : friendsList) {
					int userFriendId = Utils.toInt(tempUser);
					if (Utils.validInputPairs(userId, userFriendId)) {
						pair.set(Utils.sortedPair(userId, userFriendId));
						context.write(pair, new Text(line[1]));
					}
				}
			}
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashSet<String> localSet = new HashSet<String>();
			List<String> mutualFriends = new ArrayList<String>();

			for (Text value : values) 
			{
				String[] mapOutput = value.toString().split(Constants.COMMA);
				for (String item : mapOutput) 
				{
					if (localSet.contains(item))
						mutualFriends.add(item);
					else
						localSet.add(item);
				}
			}
			Text output = new Text();
			output.set(new Text(Utils.formatOutput(mutualFriends)));
			context.write(key, output);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Question1");
		job.setJarByClass(Question1.class);
		job.setMapperClass(Q1Map1.class);
		job.setReducerClass(ReducerClass.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
