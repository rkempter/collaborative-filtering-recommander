package ch.epfl.advanceddatabase.rkempter.rmseminimizer;
import java.io.IOException;

import ch.epfl.advanceddatabase.rkempter.initialization.TupleValueWritable;
import ch.epfl.advanceddatabase.rkempter.UVDecomposer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.join.CompositeInputSplit;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Creates the input splits andmerges M, U, V together.
 */
public class MatrixUVInputFormat extends FileInputFormat<IntWritable, InputWritable> {
	
	public static final String U_INPUT_FORMAT = "matrix.u.inputformat";
	public static final String V_INPUT_FORMAT = "matrix.v.inputformat";
	public static final String M_INPUT_FORMAT = "matrix.m.inputformat";
	public static final String U_INPUT_PATH = "matrix.u.path";
	public static final String V_INPUT_PATH = "matrix.v.path";
	public static final String M_INPUT_PATH = "matrix.m.path";
	
	// Set the U matrix Path & input class
	public static void setUInputInfo(JobConf job, Class<? extends FileInputFormat> inputFormat, String inputPath) {
		job.set(U_INPUT_FORMAT, inputFormat.getCanonicalName());
		job.set(U_INPUT_PATH, inputPath);
	}
	
	// set the M matrix path & input class
	public static void setMInputInfo(JobConf job, Class<? extends FileInputFormat> inputFormat, String inputPath) {
		job.set(M_INPUT_FORMAT, inputFormat.getCanonicalName());
		job.set(M_INPUT_PATH, inputPath);
	}
	
	// set the V matrix path & input class
	public static void setVInputInfo(JobConf job, Class<? extends FileInputFormat> inputFormat, String inputPath) {
		job.set(V_INPUT_FORMAT, inputFormat.getCanonicalName());
		job.set(V_INPUT_PATH, inputPath);
	}
	
	// Read Block-Size lines in.
	public InputSplit[] getSplits(JobConf job, int numSplits) 
			throws IOException {
		
		// Create a bloom filter
		
		// Big file, create a split for each line
		
		FileSplit[] mSplits = null;
		// get the file splits from the HDFS (1 split = 1 file)
		try {
			mSplits = getInputSplits(job, job.get(M_INPUT_FORMAT), job.get(M_INPUT_PATH), UVDecomposer.U_INPUT_BLOCK_SIZE*UVDecomposer.V_INPUT_BLOCK_SIZE);
			System.out.println("Number of mSplits: "+mSplits.length);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		FileSplit[] uSplits = null;
		try {
			uSplits = getInputSplits(job, job.get(U_INPUT_FORMAT), job.get(U_INPUT_PATH), UVDecomposer.U_INPUT_BLOCK_SIZE);
			System.out.println("Number of uSplits: "+uSplits.length);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		// Smaller file - eventually don't need to create blocks here, instead use one big split!
		FileSplit[] vSplits = null;
		try {
			vSplits = getInputSplits(job, job.get(V_INPUT_FORMAT), job.get(V_INPUT_PATH), UVDecomposer.V_INPUT_BLOCK_SIZE);
			System.out.println("Number of vSplits: "+vSplits.length);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		// Create new splits by computing the cartesian product of 
		// all blocks from U with all blocks from V
		CompositeInputSplit[] resultSplits = new CompositeInputSplit[uSplits.length * vSplits.length];
		
		int i = 0;
		
		// Splits are read in in alphabetical name order, therefore we can merge them together directly
		for(FileSplit uSplit : uSplits) {
			for(FileSplit vSplit : vSplits) {
				resultSplits[i] = new CompositeInputSplit(3);
				resultSplits[i].add(mSplits[i]);
				resultSplits[i].add(uSplit);
				resultSplits[i].add(vSplit);
				i++;
			}
			
		}
		
		return resultSplits;
	}
	
	private FileSplit[] getInputSplits(JobConf conf, String inputFormatClass, String inputPath, int numSplits) throws ClassNotFoundException, IOException {
			            // Create a new instance of the input format
		FileInputFormat inputFormat = (FileInputFormat) ReflectionUtils.newInstance(Class.forName(inputFormatClass), conf);
			            // Set the input path for the left data set
		inputFormat.setInputPaths(conf, inputPath);
			            // Get the left input splits
		return (FileSplit[]) inputFormat.getSplits(conf, numSplits); 
	}

	public RecordReader<IntWritable, InputWritable> getRecordReader (InputSplit input, JobConf job, Reporter reporter) 
			throws IOException {

			return new MatrixInputRecordReader<IntWritable, TupleValueWritable, IntWritable, MatrixInputValueWritable, IntWritable, MatrixInputValueWritable>((CompositeInputSplit) input, job, reporter);
	}
}
