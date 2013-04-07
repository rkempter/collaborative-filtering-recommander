package ch.epfl.advanceddatabase.rkempter;
import java.io.IOException;
import java.util.Random;

import ch.epfl.advanceddatabase.rkempter.initialization.*;
import ch.epfl.advanceddatabase.rkempter.rmseminimizer.*;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

public class UVDecomposer {
	
	// The size of the input matrix
	public static final int NBR_MOVIES = 17770;
	public static final int NBR_USERS = 580189;
	// Small iteration
//	public static final int NBR_MOVIES = 100;
//	public static final int NBR_USERS = 5000;
	
	public static final int D_DIMENSION = 10;
	public static final int COUNTER_MULTIPLICATOR = 10000000;
	
	// Block sizes. Smaller blocks = more parallelization (limited to around 88 tasks)
	public static final int U_INPUT_BLOCK_SIZE = 7000;
	public static final int V_INPUT_BLOCK_SIZE = 18000;
	
	// Constants to identify the matrices
	public static final int MATRIX_U = 1;
	public static final int MATRIX_V = 2;
	public static final int MATRIX_M = 3;
	
	// How many (map) tasks are used to work on the matrices
	public static final int NBR_TASKS = 88;
	
	public static final String U_INPUT_FORMAT = "decomposer.u.inputformat";
	public static final String V_INPUT_FORMAT = "decomposer.v.inputformat";
	public static final String U_MATRIX_PATH = "decomposer.u.path";
	public static final String V_MATRIX_PATH = "decomposer.v.path";
	public static final String MATRIX_X_POSITION = "decomposer.x.position";
	public static final String MATRIX_TYPE = "decomposer.matrix.type";
	
	public static String input_path;
	public static String output_path;
	//public static final String INPUT_PATH = "/std44/input/";
//	public static final String INPUT_PATH = "/netflix/input/large/";
	
	// Counters for the tasks
	public static enum OVERALL_COUNTERS {
		RMSE_COUNTER,
	};

	/**
	 * The main method, normalizes M, creates the matrix U and V and iterates over them
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		
		input_path = args[0];
		output_path = args[1];
		
		JobClient client = new JobClient();
//		JobConf initConf = getNormalizationJob();
		
		int i = 0, j = 0;
		boolean passed = true;
//		if(runJob(initConf, client)) {
			createVMatrixes();
			while(passed && i < 2) {
				JobConf uvConf;
				if(i % 2 == 0) {
					j++;
					uvConf = getUVJob(MATRIX_U, j);
				} else {
					uvConf = getUVJob(MATRIX_V, j);
				}
				
				if(runJob(uvConf, client)) {
					passed = true;
				} else {
					passed = false;
				}
				i++;
			}
//		}
	}
	
	/**
	 * Run a job, return true if the job was successful
	 * 
	 * @param job
	 * @param client
	 * @return
	 */
	private static boolean runJob(JobConf job, JobClient client) {
		client.setConf(job);
		
		try {
			JobClient.runJob(job);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 * Normalization of the Matrix and creation of the matrix U
	 * @return
	 */
	private static JobConf getNormalizationJob() {
		JobConf conf = new JobConf(UVDecomposer.class);
		
		conf.setJobName("UV Decomposer: Normalization");
		conf.setInputFormat(TupleValueInputFormat.class);
		conf.setOutputFormat(InitializationOutputFormat.class);
		
		conf.setMapOutputKeyClass(IntPair.class);
		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(TupleValueWritable.class);
		conf.setNumMapTasks(NBR_TASKS);
		int nbrTasks = (int) Math.ceil((float) NBR_USERS / U_INPUT_BLOCK_SIZE);
		conf.setNumReduceTasks(nbrTasks);
		
		FileInputFormat.addInputPath(conf, new Path(input_path));
		FileOutputFormat.setOutputPath(conf, new Path(output_path));
		
		conf.setMapperClass(InitializationMapper.class);
		conf.setOutputKeyComparatorClass(KeyComparator.class);
		conf.setPartitionerClass(BlockPartitioner.class);
		conf.setReducerClass(InitializationReducer.class);
		
		return conf;	
	}
	
	/**
	 * Creates a Job to iteratively work on U and V
	 * 
	 * @param type
	 * @param iteration
	 * @return
	 */
	private static JobConf getUVJob(int type, int iteration) {
		/* UV test */
		JobConf uvConf = new JobConf(UVDecomposer.class);
		uvConf.setJobName("UV Decomposer: Iteration "+iteration);
		
		//Set map & reducer output classes
		uvConf.setOutputKeyClass(NullWritable.class);
		uvConf.setMapOutputValueClass(InputWritable.class);
		uvConf.setMapOutputKeyClass(IntPair.class);
		uvConf.setOutputValueClass(MatrixInputValueWritable.class);
		
		int nbrTasks = (int) Math.ceil((float) NBR_USERS / U_INPUT_BLOCK_SIZE);
		uvConf.setNumReduceTasks(nbrTasks);
		
		// Set the matrix type (U or V)
		uvConf.setInt(UVDecomposer.MATRIX_TYPE, type);
		
		// Specific Input Format where U, V and M are put together to create one tuple
		MatrixUVInputFormat.setUInputInfo(uvConf, UVTupleInputFormat.class, output_path+"/U_"+(iteration-1)+"/");
		MatrixUVInputFormat.setVInputInfo(uvConf, UVTupleInputFormat.class, output_path+"/V_"+(iteration-1)+"/");
		MatrixUVInputFormat.setMInputInfo(uvConf, UVTupleInputFormat.class, output_path+"/M/");
		
		uvConf.setInputFormat(MatrixUVInputFormat.class);
		
		uvConf.setOutputFormat(MatrixUVOutputFormat.class);
		
		// Define output path depending on U or V and the iteration
		String outputPath;
		if(type == MATRIX_U) {
			outputPath = output_path+"U_"+iteration+"/";
		} else {
			outputPath = output_path+"V_"+iteration+"/";
		}
		
		FileOutputFormat.setOutputPath(uvConf, new Path(outputPath));
		
		uvConf.setMapperClass(MatrixUVMapper.class);
		uvConf.setReducerClass(MatrixUVReducer.class);
		
		return uvConf;
	}
	
	private static void createVMatrixes() {
		// Create U, V matrices
		
		JobConf conf = new JobConf(UVDecomposer.class);
				
		try {
			// Create V matrix - @Todo: Eventually need to write custom method to store column wise!!
			createMatrix(conf, NBR_MOVIES, MATRIX_V);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Creation of Matrix V using randomized
	 * values between 0 and 1 (uniformly distributed)
	 * 
	 * @param conf
	 * @param index
	 * @param type
	 * @throws IOException
	 */
	private static void createMatrix(JobConf conf, int index, int type) throws IOException {
		Random generator = new Random();
		
		FileSystem fs = FileSystem.get(conf);
		
		String outputTuple = "<%s,%d,%d,%f>\n";
		int blocks = 0, blockSize = 0, total = 0;
		String startPath;
		blocks = (int) Math.ceil((float) NBR_MOVIES / V_INPUT_BLOCK_SIZE);
		blockSize = V_INPUT_BLOCK_SIZE;
		startPath = output_path+"V_0/";
		total = NBR_USERS;
		
		// generate matrix V
		for(int fileName = 0; fileName < blocks; fileName++) {
			String file = String.format("part-%05d", fileName);
			Path filePath = new Path(startPath+file);
			
			FSDataOutputStream out = fs.create(filePath);
			if(total < blockSize) {
				blockSize = total;
			}
			for(int i = 1; i <= blockSize; i++) {
				for(int j = 1; j <= D_DIMENSION; j++) {
					float value = (float) Math.abs(generator.nextGaussian());
					String output;
					output = String.format(outputTuple, "V", j, i + fileName * UVDecomposer.V_INPUT_BLOCK_SIZE, value);
					out.writeBytes(output);
				}
			}
			total -= blockSize;
			out.close();
		}
		
		fs.close();
	}
}
