import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * Task a 
 * Write a job(s) that reports all Facebook users (name,and hobby) whose 
 * Nationality is the same as your own Nationality (pick one: be it Chinese
 * or German or whatever).
 * 
 * input: my_page 
 * output: Chinese Facebook users (name and hobby) 	 
 */


/*
task_a
This is a map-only job. We need to find people with the same nationality(Chinese in this case).
input: my_page
output: Chinese Facebook users (name and hobby) 	
 
Here is the process:
Implement map, process one line at a time, as provided by the specified TextInput Format.
Split the line into splits separated by ,
Output key-value pairs output.collect(one, word), i.e. (1, ID, nationality).

In this case, we don't need reducer. Thus, the output of task_a is a list of Facebook users whose nationality is Chinese.
 */


public class task_a{
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, Text> {
			//mapper implementation
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reportor)
				throws IOException {
			String line = value.toString();
			String[] tokens = line.split(",");
			String result;
			if (tokens[3].equals("2")) {//finds out whose nationality is 2(chinese)
				result = tokens[1] + " " + tokens[4];// name [1] and hobby [4]
				word.set(result);
				output.collect(one, word);
			}
		}
		// No need for reducer
	}

	
	
	public static void main(String[] args) throws Exception{
		JobConf job = new JobConf(task_a.class);
		
		job.setMapperClass(Map.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		JobClient.runJob(job);
	}

}
