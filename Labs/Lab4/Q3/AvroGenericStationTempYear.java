import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvroGenericStationTempYear extends Configured implements Tool
{

	private static Schema SCHEMA;

	public static class AvroMapper extends Mapper<LongWritable, Text, AvroKey<GenericRecord>, NullWritable>
	{
		private NcdcLineReaderUtils utils = new NcdcLineReaderUtils();
		private GenericRecord record = new GenericData.Record(SCHEMA);
		AvroKey<GenericRecord> avroKey = new AvroKey<GenericRecord>(record);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			utils.parse(value.toString());

			if (utils.isValidTemperature())
			{
				record.put("stationId", utils.getStationId());
				record.put("temperature", utils.getAirTemperature());
				record.put("year", utils.getYearInt());
				
				avroKey.datum(record);
				
				context.write(avroKey, NullWritable.get());
				//System.out.println(key+":"+utils.getStationId()+":"+utils.getAirTemperature()+":"+utils.getYearInt());
			}
		}
	}

	public static class AvroReducer extends Reducer<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, NullWritable>
	{
		private GenericRecord record;
		@Override
		protected void reduce(AvroKey<GenericRecord> key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException
		{
			GenericRecord record0 = new GenericData.Record(SCHEMA);
			AvroKey<GenericRecord> MaxTempKey = new AvroKey(record0);
			boolean first = true;
			float maxTemp = 0;
			float temp1 = 0;
			for(NullWritable value: values){
				record = key.datum();
				temp1 = (float) record.get("temperature");
				if(first||(maxTemp<temp1)){
					first=false;
					record0.put("stationId", record.get("stationId"));
					record0.put("temperature", record.get("temperature"));
					record0.put("year", record.get("year"));
					maxTemp = (float) (record0.get("temperature"));
				}
			}
			context.write(MaxTempKey, NullWritable.get());
			System.out.println(MaxTempKey);
		}
	}

	@Override
	public int run(String[] args) throws Exception
	{
		if (args.length != 3)
		{
			System.err.printf("Usage: %s [generic options] <input> <output> <schema-file>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Job job = Job.getInstance();
		job.setJarByClass(AvroGenericStationTempYear.class);
		job.setJobName("Avro Station-Temp-Year");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		String schemaFile = args[2];

		SCHEMA = new Schema.Parser().parse(new File(schemaFile));

		job.setMapperClass(AvroMapper.class);
		job.setCombinerClass(AvroReducer.class);
		job.setReducerClass(AvroReducer.class);
		
		job.setMapOutputValueClass(NullWritable.class);

		AvroJob.setMapOutputKeySchema(job, SCHEMA);
		AvroJob.setOutputKeySchema(job, SCHEMA);

		job.setOutputFormatClass(AvroKeyOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		FileSystem.get(conf).delete(new Path("output"), true);
		int res = ToolRunner.run(conf, new AvroGenericStationTempYear(), args);		
		System.exit(res);
	}
}