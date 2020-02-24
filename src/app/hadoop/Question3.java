package app.hadoop;

/*
 *Use in-memory join to answer this question.
 *		Given any two Users (they are friends) as input, output the list of the names and the date of birth (mm/dd/yyyy) 
 *		of their mutual friends. Use the userdata.txt to get the extra user information.
 *Output format:
 *		UserA id, UserB id, list of [names: date of birth (mm/dd/yyyy)] of their mutual Friends.
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
import java.util.HashMap;
import java.util.HashSet;

public class Question3 {
	
	public static class Q3Map1 extends Mapper<LongWritable, Text, Text, Text> {

		static HashMap<Integer, String> userdataMap = new HashMap<Integer, String>();
		
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path part = new Path(context.getConfiguration().get(Constants.USERDATA));
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
						userdataMap.put(Utils.toInt(info[0]), info[1] + Constants.SEPARATOR + info[9]);
					}
					line = br.readLine();
				}
			}
		}
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			Configuration tempConf = context.getConfiguration();		
			String[] line = values.toString().split(Constants.TAB);
			if (line.length == 2)
			{
				StringBuilder sb;
				int userId = Utils.toInt(line[0]);
				String[] friendsList = line[1].split(Constants.COMMA);
				Text pair = new Text();
				for (String tempUser : friendsList) 
				{
					int userFriendId = Utils.toInt(tempUser);
					if (Utils.validInputPairsQ3(userId, userFriendId,Utils.toInt(tempConf.get(Constants.INPUT_1)),Utils.toInt(tempConf.get(Constants.INPUT_2)))) 
					{
						sb = new StringBuilder();
						pair.set(Utils.sortedPair(userId, userFriendId));
						for (String temp : friendsList) 
						{
							int tempid = Utils.toInt(temp);
							sb.append(tempid+Constants.SEPARATOR+userdataMap.get(tempid) + Constants.COMMA);
						}
						context.write(pair, new Text(Utils.formatOutput(sb)));
					}
				}
			}
		}
	}

	public static class Q3Reduce1 extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {			
			HashSet<String> localSet = new HashSet<String>();
			StringBuilder mutual = new StringBuilder();

			for (Text value : values) 
			{
				String[] mapOutput = value.toString().split(Constants.COMMA);
				for (String item : mapOutput) 
				{
					if (localSet.contains(item))
					{
						String[] info = item.split(Constants.SEPARATOR);
						mutual.append(info[1]+Constants.SEPARATOR+info[2]+ Constants.COMMA);
					}
					else
						localSet.add(item);
				}
			}
			Text output = new Text();
			output.set(new Text(Utils.formatOutput(mutual)));
			context.write(key, output);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf1 = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();

		conf1.set(Constants.INPUT_1, otherArgs[4]);
		conf1.set(Constants.INPUT_2, otherArgs[5]);
		conf1.set(Constants.USERDATA, otherArgs[2]);

		Job job1 = Job.getInstance(conf1, "Question3");

		job1.setJarByClass(Question3.class);
		job1.setMapperClass(Q3Map1.class);
		job1.setReducerClass(Q3Reduce1.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[3]));

		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}
	}

}
