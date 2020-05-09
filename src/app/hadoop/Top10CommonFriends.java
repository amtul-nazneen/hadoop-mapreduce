package app.hadoop;

/*
 * Find friend pairs whose common friend number are within the top-10 in all the pairs.
 * 		Output them in decreasing order.
 * Output Format:
 * 		<User_A>, <User_B><TAB><Mutual/Common Friend Number>
 * */

/**
 * @author amtulnazneen
 *
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import utils.Constants;
import utils.Utils;
import java.io.IOException;
import java.util.HashSet;

public class Top10CommonFriends {

	/* ----------------------------------- Job 1 ----------------------------------- */
	public static class Q2Map1 extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {

			String[] line = values.toString().split(Constants.TAB);
			if (line.length == 2)
			{
				int userId = Utils.toInt(line[0]);
				String[] friendsList = line[1].split(Constants.COMMA);
				Text pair = new Text();
				for (String tempUser : friendsList) {
					int userFriendId = Utils.toInt(tempUser);
						pair.set(Utils.sortedPair(userId, userFriendId));
						context.write(pair, new Text(line[1]));
				}
			}
		}
	}

	public static class Q2Reduce1 extends Reducer<Text, Text, Text, IntWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashSet<String> localSet = new HashSet<String>();
			int count = 0;

			for (Text value : values) {
				String[] mapOutput = value.toString().split(Constants.COMMA);
				for (String item : mapOutput) {
					if (localSet.contains(item))
						count++;
					else
						localSet.add(item);
				}
			}
			context.write(key, new IntWritable(count));
		}
	}

	/* ----------------------------------- Job 1 ----------------------------------- */
	public static class Q2Map2 extends Mapper<Text, Text, LongWritable, Text> {

		private LongWritable count = new LongWritable();

		public void map(Text key, Text values, Context context) throws IOException, InterruptedException {
			int newVal = Utils.toInt(values.toString());
			count.set(newVal);
			context.write(count, key);
		}
	}

	public static class Q2Reduce2 extends Reducer<LongWritable, Text, Text, LongWritable> {
		private int count = 0;

		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				if (count < Constants.TOP_LIMIT) {
					count++;
					context.write(value, key);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
			Configuration conf1 = new Configuration();
			Job job1 = Job.getInstance(conf1, "Top10CommonFriends_1");

			job1.setJarByClass(Top10CommonFriends.class);
			job1.setMapperClass(Q2Map1.class);
			job1.setReducerClass(Q2Reduce1.class);

			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);

			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(job1, new Path(args[1]));
			FileOutputFormat.setOutputPath(job1, new Path(args[2]));

			if (!job1.waitForCompletion(true)) {
				System.exit(1);
			}
				Configuration conf2 = new Configuration();
				Job job2 = Job.getInstance(conf2, "Top10CommonFriends_2");

				job2.setJarByClass(Top10CommonFriends.class);
				job2.setMapperClass(Q2Map2.class);
				job2.setReducerClass(Q2Reduce2.class);

				job2.setMapOutputKeyClass(LongWritable.class);
				job2.setMapOutputValueClass(Text.class);

				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(LongWritable.class);
				
				job2.setInputFormatClass(KeyValueTextInputFormat.class);
				job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
				job2.setNumReduceTasks(1);
				
				FileInputFormat.addInputPath(job2, new Path(args[2]));
				FileOutputFormat.setOutputPath(job2, new Path(args[3]));

				System.exit(job2.waitForCompletion(true) ? 0 : 1);

	}

}