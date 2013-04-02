package ch.epfl.advanceddatabase.rkempter;
import java.io.IOException;
import java.util.Random;

import ch.epfl.advanceddatabase.rkempter.initialization.*;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class UVDecomposer {
	
	public static final int NBR_MOVIES = 100;
	public static final int NBR_USERS = 5000;
	public static final int D_DIMENSION = 10;
	public static final int BLOCK_SIZE = 1000;
	public static final int COUNTER_MULTIPLICATOR = 10000000;
	
	public static final int MATRIX_U = 1;
	public static final int MATRIX_V = 2;
	public static final int MATRIX_M = 3;
	public static final float MATRIX_X_MARKER = -10;
	
	public static final String U_MATRIX_INPUT_FORMAT = "decomposer.u.inputformat";
	public static final String V_MATRIX_INPUT_FORMAT = "decomposer.v.inputformat";
	public static final String U_MATRIX_PATH = "decomposer.u.path";
	public static final String V_MATRIX_PATH = "decomposer.v.path";
	public static final String MATRIX_X_POSITION = "decomposer.x.position";
	public static final String MATRIX_TYPE = "decomposer.matrix.type";
	
	// Counters for the tasks
	public static enum OVERALL_COUNTERS {
		RMSE_COUNTER
	};

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(UVDecomposer.class);
		
		conf.setJobName("UV Decomposer");
		conf.setInputFormat(TupleValueInputFormat.class);
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(TupleValueWritable.class);
		
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
			createMatrix(conf, uPath, NBR_USERS, "U");
			// Create V matrix - @Todo: Eventually need to write custom method to store column wise!!
			createMatrix(conf, vPath, NBR_MOVIES, "V");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	private static void createMatrix(JobConf conf, Path path, int index, String type) throws IOException {
		Random generator = new Random();
		
		FileSystem fs = FileSystem.get(conf);
		
		if(fs.exists(path)) {
			System.out.println("Matrix File already exists");
			return;
		}
		
		FSDataOutputStream out = fs.create(path);
		
		// generate matrix
		
		for(int i = 0; i < index; i++) {
			for(int j = 0; j < D_DIMENSION; j++) {
				float value = (float) generator.nextGaussian();
				String output;
				if(type == "U") {
					output = String.format("<%s, %d, %d, %f>\n", type, i, j, value);
				} else {
					output = String.format("<%s, %d, %d, %f>\n", type, j, i, value);
				}
				out.writeChars(output);
			}
		}
		
		
		out.close();
		fs.close();
	}
}
