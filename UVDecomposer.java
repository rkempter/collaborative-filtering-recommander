import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class UVDecomposer {
	
	public static class InitializationMapper extends MapReduceBase implements Mapper<IntWritable, TupleValue, IntWritable, TupleValue>
	{
		public void map(IntWritable key, TupleValue value, OutputCollector<IntWritable, TupleValue> output, Reporter reporter) throws IOException {
			
			// Identity function
			output.collect(key, value);
		}
	}
	
	public static class InitializationReducer extends MapReduceBase implements Reducer<IntWritable, TupleValue, IntWritable, TupleValue>
	{
		public void reduce(IntWritable key, Iterator<TupleValue> values,
				OutputCollector<IntWritable, TupleValue> output, Reporter reporter)
				throws IOException {
			
				int sum = 0;
				int counter = 0;
				ArrayList<TupleValue> tupleValueList= new ArrayList<TupleValue>();
				
				// Compute the average of the user rating
				while (values.hasNext()) {
					TupleValue tv = values.next();
					TupleValue tupleValue = new TupleValue(tv.getIndex(), tv.getGrade());
					tupleValueList.add(tupleValue);
					sum += tv.getGrade();
					counter++;
				}
				
				float avg = (float) sum / counter;
				
				// Write adjusted matrix row out
				for(int i = 0; i < tupleValueList.size(); i++) {
					TupleValue value = tupleValueList.get(i);
					System.out.println(key + " " + value.getIndex() + " " + value.getGrade());
					float grade = value.getGrade() - avg;
					value.setGrade(grade);
					output.collect(key, value);
				}
				
		}
	}
	

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(UVDecomposer.class);
		
		conf.setJobName("UV Decomposer");
		conf.setInputFormat(TupleValueInputFormat.class);
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(TupleValue.class);
		
		String inputPath = "/std44/input/";
		String outputPath = "/std44/output/";
		
		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		
		conf.setMapperClass(InitializationMapper.class);
		conf.setReducerClass(InitializationReducer.class);
		
		client.setConf(conf);
		
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
