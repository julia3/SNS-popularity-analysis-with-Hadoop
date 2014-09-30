import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;


/**
 * Task d 
 * For each facebook page compute the happiness factor of its owner. 
 * That is, for each facebook page in your dataset, report the owner's 
 * name, and the number of people listing him or her as friend.
 *  
 *  input: my_page, friend_page
 *  output: name, number of friends
 */


/*
This is a map-reduce job. We need to report the owner's name, and the number of people listing him or her as friend.
input: my_page, friend_page
output: name, number of friends

Here is the process:
Implement my_pageMap, process one line at a time, as provided by the specified TextInput Format.
Define a filetag to specify my_pageMap
Split the line into splits separated by ,
Output key-value pairs output.collect(ID,new Text(fileTag + name)), i.e. (M, ID, name).

Implement friend_pageMap, process one line at a time, as provided by the specific specified TextInput Format.
Split the line into splits separated by ,
Define a filetag to specify friend_pageMap.
Output key-value pairs output.collect(ID,new Text(fileTag + MyFriend)), i.e. (0, F, MyFriend).

Impement reducer.
Via the reduce method just count how many friends a person has, define a variable to store it.
While values.hasNext && in friend_page, FriendNum++.
Output key-value pairs output.collect(key,text), i.e. (name, FriendNum).
 */



public class task_d{
	/*
	Mapper implementation of my_pageMap, output filetag and user name
	*/
	public static class my_pageMap extends MapReduceBase implements 
			Mapper<LongWritable, Text, IntWritable, Text>{
		
       	private  IntWritable ID = new IntWritable(0);
		private String name, fileTag ="M,";//define a filetag to specify my_pageMap
		
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> 
				output, Reporter reporter)throws IOException{
			String line = value.toString();
			String[] splits = line.split(",");
			ID.set(Integer.parseInt(splits[0]));
			name = splits[1];
			output.collect(ID,new Text(fileTag + name));
			//output of my_pageMap
		}
	}
	

	/*
	Mapper implementation of friend_pageMap, output filetag and friends' name
	*/
	public static class friend_pageMap extends MapReduceBase implements 
			Mapper<LongWritable, Text, IntWritable, Text>{
		private IntWritable ID = new IntWritable(0);
		private String MyFriend, fileTag ="F,";//define a filetag to specify friend_pageMap
		
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> 
				output, Reporter reporter)throws IOException{
			String line = value.toString();
			String[] splits = line.split(",");
			ID.set(Integer.parseInt(splits[1]));
			MyFriend = splits[2];
			output.collect(ID,new Text(fileTag + MyFriend));
			//output of friend_pageMap
		}
	}


	/*
	Reducer implementation, combine the result of two mappers
	*/
	public static class Reduce extends MapReduceBase implements 
			Reducer<IntWritable, Text, IntWritable, Text>{
		
		private String name, MyFriend;
		private long FriendNum; //count how many friends a person has
		public void reduce(IntWritable key, Iterator<Text> values, 
					OutputCollector<IntWritable, Text> output, Reporter reporter)
					throws IOException{
			FriendNum = 0;
			while (values.hasNext()){
				String line = values.next().toString();
				String splits[] = line.split(",");
			
				if(splits[0].equals("M")){
					name = splits[1];
				}
				else if(splits[0].equals("F")){
					MyFriend = splits[1];
					FriendNum += 1;
				}
			}
			Text text = new Text();
			text.set(name+"," + Long.toString(FriendNum));
			output.collect(key,text);	
		}
	}

	public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(task_d.class);
      conf.setJobName("task_d");

      conf.setOutputKeyClass(IntWritable.class);
      conf.setOutputValueClass(Text.class);

      conf.setReducerClass(Reduce.class);

      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);
	
      MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, my_pageMap.class);
      MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, friend_pageMap.class);
      FileOutputFormat.setOutputPath(conf, new Path(args[2]));

      JobClient.runJob(conf);
    }
}
