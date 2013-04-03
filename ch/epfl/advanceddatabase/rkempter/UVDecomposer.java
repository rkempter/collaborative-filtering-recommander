package ch.epfl.advanceddatabase.rkempter;
import java.io.IOException;
import java.util.Random;

import ch.epfl.advanceddatabase.rkempter.initialization.*;
import ch.epfl.advanceddatabase.rkempter.rmseminimizer.*;
import ch.epfl.advanceddatabase.rkempter.filemerger.*;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.hash.Hash;

public class UVDecomposer {
	
	public static final int NBR_MOVIES = 100;
	public static final int NBR_USERS = 5000;
	public static final int D_DIMENSION = 10;
	public static final int BLOCK_SIZE = 100;
	public static final int COUNTER_MULTIPLICATOR = 10000000;
	public static final int U_INPUT_BLOCK_SIZE = 2500;
	
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
	public static final String U_PATH = "/std44/output/U_i/umatrix.txt";
	public static final String V_PATH = "/std44/output/V_i/vmatrix.txt";
	public static final String M_PATH = "/std44/output/M/";
	public static final String I_PATH = "/std44/output/I/";
	public static final String INPUT_PATH = "/std44/input/";
	
	
	public static final int NUM_NONBLANK = 18837;//98370417
	
	// Counters for the tasks
	public static enum OVERALL_COUNTERS {
		RMSE_COUNTER
	};
	

	public static void main(String[] args) {
		JobClient client = new JobClient();
		
		// Bloom filter
		float falsePosRate = (float) 0.1;
		int vectorSize = getOptimalBloomFilterSize(NUM_NONBLANK, falsePosRate);
		int nbHash = getOptimalK(NUM_NONBLANK, vectorSize);
		
		BloomFilter filter = new BloomFilter(vectorSize, nbHash, Hash.MURMUR_HASH);
		
		
		
		JobConf conf = new JobConf(UVDecomposer.class);
		
		conf.setJobName("UV Decomposer");
		conf.setInputFormat(TupleValueInputFormat.class);
		conf.setOutputFormat(InitializationOutputFormat.class);
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(TupleValueWritable.class);
		
		// Specify output for normalized matrix
		//MultipleOutputs.addNamedOutput(conf, "norm-matrix", TextOutputFormat.class, IntWritable.class, TupleValue.class);
		
		String inputPath = INPUT_PATH;
		String outputPath = M_PATH;
		
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
//		
		/* UV test */
		int xPos = 2;
		JobConf uvConf = new JobConf(UVDecomposer.class);
		uvConf.setJobName("UV Conf");
		
		//Set map & reducer output classes
		uvConf.setOutputKeyClass(IntWritable.class);
		uvConf.setMapOutputValueClass(MatrixUVValueWritable.class);
		uvConf.setOutputValueClass(MatrixInputValueWritable.class);
		uvConf.setCombinerClass(MatrixCombiner.class);
		
		// Set the matrix x position and the matrix type
		uvConf.setInt(UVDecomposer.MATRIX_X_POSITION, xPos);
		uvConf.setInt(UVDecomposer.MATRIX_TYPE, MATRIX_U);
		
		MatrixUVInputFormat.setUInputInfo(uvConf, UVTupleInputFormat.class, "/std44/output/U_i/");
		MatrixUVInputFormat.setVInputInfo(uvConf, UVTupleInputFormat.class, "/std44/output/V_i/");
		
		//uvConf.setInputFormat(MatrixUVInputFormat.class);
		uvConf.setOutputFormat(MatrixUVOutputFormat.class);
		
		MultipleInputs.addInputPath(uvConf, new Path("/std44/output/U_i/"), MatrixUVInputFormat.class, MatrixUVMapper.class);
		MultipleInputs.addInputPath(uvConf, new Path(UVDecomposer.M_PATH), TupleValueInputFormat.class, MatrixMMapper.class);
		
		FileOutputFormat.setOutputPath(uvConf, new Path(UVDecomposer.I_PATH));
			
		uvConf.setReducerClass(MatrixUVReducer.class);
		
		client.setConf(uvConf);
		
		
		
		// First element
		
		//uvConf = MatrixUVDriver.getNewUVConf(uvConf, MATRIX_U, 1);
		try {
			JobClient.runJob(uvConf);
		} catch (Exception e) {
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
		for(int i = 1; i <= index; i++) {
			for(int j = 1; j <= D_DIMENSION; j++) {
				float value = (float) generator.nextGaussian() / 10;
				String output;
				if(type == "U") {
					output = String.format("<%s,%d,%d,%f>\n", type, i, j, value);
				} else {
					output = String.format("<%s,%d,%d,%f>\n", type, j, i, value);
				}
				out.writeBytes(output);
			}
		}
		
		out.close();
		fs.close();
	}
	
	public static int getOptimalBloomFilterSize(int numRecords,
			float falsePosRate) {
		int size = (int) (-numRecords * (float) Math.log(falsePosRate) / Math
				.pow(Math.log(2), 2));
		return size;
	}

	public static int getOptimalK(float numMembers, float vectorSize) {
		return (int) Math.round(vectorSize / numMembers * Math.log(2));
	}
}
