import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ClimateData extends Configured implements Tool
{
	public static class ClimateDataMapper extends Mapper<LongWritable, Text, Text, Pair>//
	{
		private Pair pair = new Pair();
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String year = value.toString().substring(15, 19);
			pair = new Pair(new IntWritable(Integer.parseInt(value.toString().substring(87, 92))),one);
			word.set(year);
			//System.out.println(key+":"+pair.getValue());
			context.write(word, pair);//
		}
	}

	public static class ClimateDataCombiner extends Reducer<Text, Pair, Text, Pair>
	{
		//private Map<Text, Pair<IntWritable,IntWritable>> tempMap = new HashMap();
		
		@Override
		public void reduce(Text key, Iterable<Pair> pairs, Context context) throws IOException, InterruptedException{
			Map<Text, Pair> tempMap = new HashMap();
			for(Pair pair:pairs){
				Pair p = tempMap.get(key);
				if(p!=null){
					int count = pair.getValue().get();
					count+= p.getValue().get();
					pair.setValue(new IntWritable(count));
				}
				System.out.println(key+":"+pair.getValue());
				tempMap.put(key, pair);
			}
			context.write(key, tempMap.get(key));
		}
	}
	
	public static class ClimateDataReducer extends Reducer<Text, Pair, Text, DoubleWritable>
	{
		private DoubleWritable result = new DoubleWritable();
		
		@Override
		public void reduce(Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException
		{
			int temp = 0;
			int cnt = 0;
			for(Pair val: values){
				temp+=val.getKey().get();
				cnt+=val.getValue().get();
				//System.out.println(key+":"+val);
			}
			result.set(temp/cnt/10);
			context.write(key, result);
		}
		
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new ClimateData(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{
		Configuration c = getConf();
		// configuration should contain reference to your namenode
		FileSystem fs = FileSystem.get(c);
		// true stands for recursively deleting the folder you gave
		if(fs.exists(new Path(args[2])))
			fs.delete(new Path(args[2]), true);
		
		Job job = new Job(c, "ClimateDataJob");
		job.setJarByClass(ClimateData.class);

		job.setMapperClass(ClimateDataMapper.class);
		job.setCombinerClass(ClimateDataCombiner.class);
		job.setReducerClass(ClimateDataReducer.class);
		job.setNumReduceTasks(1);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Pair.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
