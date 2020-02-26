package app.hadoop;

/*
 * Use reduce-side join and job chaining:
 * 		Calculate the maximum age of the direct friends of each user.
 * 		Sort the users based on the calculated maximum age in descending order as described in step 1.
 * 		Output the top 10 users with their address and the calculated maximum age.
 * */

/**
 * @author amtulnazneen
 *
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.HashMap;

public class Question4 {
	
	/* ----------------------------------- Job 1 ----------------------------------- */
	public static class Q4Map1 extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] line = values.toString().split(Constants.TAB);
			if (line.length == 2)
			{
				context.write(new Text(line[0]), new Text(line[1]));
			}
		}
	}

	public static class Q4Reduce1 extends Reducer<Text, Text, Text, Text> {

		static HashMap<Integer, Integer> userAgeMap = new HashMap<Integer, Integer>();

		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path part = new Path(conf.get(Constants.USERDATA));
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
			for (FileStatus status : fss) {
				Path pt = status.getPath();
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line;
				line = br.readLine();
				while (line != null) {
					String[] info = line.split(Constants.COMMA);
					if (info.length == 10) {
						try {
							int age = Utils.computeAge(info[9]);
							userAgeMap.put(Utils.toInt(info[0]), age);
						} catch (ParseException e) {
							e.printStackTrace();
						}
					}
					line = br.readLine();
				}
			}
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text tuples : values) {
				String[] friends = tuples.toString().split(Constants.COMMA);
				int maxAge=Constants.MAX_AGE;
				for (String user : friends) {
				int	age = userAgeMap.get(Utils.toInt(user));
					if (age > maxAge) {
						maxAge = age;
					}
				}
				context.write(key, new Text(Integer.toString(maxAge)));
			}
		}
	}

	/* ----------------------------------- Job 2 ----------------------------------- */
	public static class Q4Map2 extends Mapper<LongWritable, Text, LongWritable, Text> {
		private LongWritable count = new LongWritable();

		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] info = values.toString().split(Constants.TAB);
			if (info.length == 2) {
				count.set(Long.parseLong(info[1]));
				context.write(count, new Text(info[0]));
			}
		}
	}

	public static class Q4Reduce2 extends Reducer<LongWritable, Text, Text, Text> {
		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text tuples : values) {
				context.write(tuples, new Text(Long.toString(key.get())));
			}
		}
	}
	
	/* ----------------------------------- Job 3 ----------------------------------- */
	public static class Q4Map3 extends Mapper<LongWritable, Text, Text, Text> {
		private int count = 0;

		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			if (count < Constants.TOP_LIMIT) {
				String[] info = values.toString().split(Constants.TAB);
				if (info.length == 2) {
					count++;
					context.write(new Text(info[0]), new Text(info[1]));
				}
			}
		}
	}
	
	public static class Q4Reduce3 extends Reducer<Text, Text, Text, Text> {
		static HashMap<Integer, String> userDataMap = new HashMap<Integer, String>();

		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path part = new Path(conf.get(Constants.USERDATA));
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
			for (FileStatus status : fss) {
				Path pt = status.getPath();
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line;
				line = br.readLine();
				while (line != null) {
					String[] info = line.split(Constants.COMMA);
					if (info.length == 10) {
						userDataMap.put(Integer.parseInt(info[0]), info[0]+","+info[3]+","+info[4]+","+info[5]+","+info[6]+","+info[7]);
					}
					line = br.readLine();
				}
			}
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int userId = Utils.toInt(key.toString());
			String userInfo = userDataMap.get(userId);
			for (Text tuples : values) {
				context.write(new Text(userInfo), tuples);
			}
		}
	}
	
	

	public static void main(String[] args) throws Exception {
		Configuration conf1 = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();

		conf1.set(Constants.USERDATA, otherArgs[2]);
		Job job1 = Job.getInstance(conf1, "Question4_1");

		job1.setJarByClass(Question4.class);
		job1.setMapperClass(Q4Map1.class);
		job1.setReducerClass(Q4Reduce1.class);

		FileInputFormat.addInputPath(job1, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[3]));

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Question4_2");

		job2.setJarByClass(Question4.class);
		job2.setMapperClass(Q4Map2.class);
		job2.setReducerClass(Q4Reduce2.class);

		FileInputFormat.addInputPath(job2, new Path(otherArgs[3]));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[4]));

		job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		if (!job2.waitForCompletion(true)) {
			System.exit(1);
		}
		
		Configuration conf3 = new Configuration();
		conf3.set(Constants.USERDATA, otherArgs[2]);
		Job job3 = Job.getInstance(conf3, "Question4_3");

		job3.setJarByClass(Question4.class);
		job3.setMapperClass(Q4Map3.class);
		job3.setReducerClass(Q4Reduce3.class);

		FileInputFormat.addInputPath(job3, new Path(otherArgs[4]));
		FileOutputFormat.setOutputPath(job3, new Path(otherArgs[5]));

		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		if (!job3.waitForCompletion(true)) {
			System.exit(1);
		}
	}
}
