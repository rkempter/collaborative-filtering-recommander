import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
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
	
	public static final int NBR_MOVIES = 100;
	public static final int NBR_USERS = 5000;
	public static final int D_DIMENSION = 10;
	public static final int BLOCK_SIZE = 1000;
	
	public static final String U_MATRIX_INPUT_FORMAT = "decomposer.u.inputformat";
	public static final String V_MATRIX_INPUT_FORMAT = "decomposer.v.inputformat";
	public static final String U_MATRIX_PATH = "decomposer.u.path";
	public static final String V_MATRIX_PATH = "decomposer.v.path";
	
	public static class InitializationMapper extends MapReduceBase implements Mapper<IntWritable, TupleValue, IntWritable, TupleValue>
	{
		public void map(IntWritable key, TupleValue value, OutputCollector<IntWritable, TupleValue> output, Reporter reporter) throws IOException {
			
			// Identity function
			output.collect(key, value);
		}
	}
	
	public static class InitializationReducer extends MapReduceBase implements Reducer<IntWritable, TupleValue, IntWritable, TupleValue>
	{
		
//		private MultipleOutputs mos;
//		
//		public void setup(JobConf conf) {
//			
//			mos = new MultipleOutputs(conf);
//			
//		}
		
		public void reduce(IntWritable key, Iterator<TupleValue> values,
				OutputCollector<IntWritable, TupleValue> output, Reporter reporter)
				throws IOException {
			
				int sum = 0;
				int counter = 0;
				ArrayList<TupleValue> tupleValueList= new ArrayList<TupleValue>();
				
				// Compute the average of the user rating
				while (values.hasNext()) {
					// Hate this!
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
					float grade = value.getGrade() - avg;
					value.setGrade(grade);
					output.collect(key, value);
				}
				
				// Need to write U and V
		}
		
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//	          mos.close();
//	    }
	}
	

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(UVDecomposer.class);
		
		conf.setJobName("UV Decomposer");
		conf.setInputFormat(TupleValueInputFormat.class);
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(TupleValue.class);
		
		// Specify output for normalized matrix
		//MultipleOutputs.addNamedOutput(conf, "norm-matrix", TextOutputFormat.class, IntWritable.class, TupleValue.class);
		
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
		
		// Create U, V matrices
		Path uPath = new Path("/std44/output/U_i/umatrix.txt");
		Path vPath = new Path("/std44/output/V_i/vmatrix.txt");
		
		try {
			// Create U matrix
			createMatrix(conf, uPath, NBR_USERS, D_DIMENSION, "U");
			// Create V matrix - @Todo: Eventually need to write custom method to store column wise!!
			createMatrix(conf, vPath, NBR_MOVIES, D_DIMENSION, "V");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	private static void createMatrix(JobConf conf, Path path, int index1, int index2, String type) throws IOException {
		Random generator = new Random();
		
		FileSystem fs = FileSystem.get(conf);
		
		if(fs.exists(path)) {
			System.out.println("Matrix File already exists");
			return;
		}
		
		FSDataOutputStream out = fs.create(path);
		
		// generate matrix
		
		for(int k = 0; k < (index1 / BLOCK_SIZE); k++) {
			for(int i = 0; i < D_DIMENSION; i++) {
				for(int j = k * BLOCK_SIZE+1; j <= (k+1) * BLOCK_SIZE; j++) {
					float value = (float) generator.nextGaussian();
					String output;
					if(type == "U") {
						output = String.format("<%s, %d, %d, %f>\n", type, j, i, value);
					} else {
						output = String.format("<%s, %d, %d, %f>\n", type, i, j, value);
					}
					out.writeChars(output);
				}
			}
		}
		
		
		out.close();
		fs.close();
	}
}
