import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.join.CompositeInputSplit;


public class MatrixInputFormat extends FileInputFormat<MatrixInputValueWritable, MatrixInputValueWritable> {
	
	private static String U_INPUT_FORMAT = "matrix.u.inputformat";
	private static String V_INPUT_FORMAT = "matrix.v.inputformat";
	private static String U_INPUT_PATH = "matrix.u.path";
	private static String V_INPUT_PATH = "matrix.v.path";
	
	public static void setUInputInfo(JobConf job, Class<? extends FileInputFormat> inputFormat, String inputPath) {
		job.set(U_INPUT_FORMAT, inputFormat.getCanonicalName());
		job.set(U_INPUT_PATH, inputPath);
	}
	
	public static void setVInputInfo(JobConf job, Class<? extends FileInputFormat> inputFormat, String inputPath) {
		job.set(V_INPUT_FORMAT, inputFormat.getCanonicalName());
		job.set(V_INPUT_PATH, inputPath);
	}
	
	// Read Block-Size lines in.
	public InputSplit[] getSplits(JobConf job, int numSplits) 
			throws IOException {
		
		// Get the FileStatus
		String vInputPathString = job.get(V_INPUT_PATH);
		String uInputPathString = job.get(U_INPUT_PATH);
		Path vInputPath = new Path(vInputPathString);
		FileSystem fs = vInputPath.getFileSystem(job);
		FileStatus vStatus = fs.getFileStatus(vInputPath);
		FileStatus uStatus = fs.getFileStatus(new Path(uInputPathString));
		
		// Big file, create a split for each block
		InputSplit[] uSplits = getBlockLineSplits(job, uStatus, UVDecomposer.BLOCK_SIZE);
		
		// Smaller file - eventually don't need to create blocks here, instead use one big split!
		InputSplit[] vSplits = getBlockLineSplits(job, vStatus, UVDecomposer.BLOCK_SIZE);
		
		// Create new splits by computing the cartesian product of 
		// all blocks from U with all blocks from V
		CompositeInputSplit[] resultSplits = new CompositeInputSplit[uSplits.length * vSplits.length];
		
		int i = 0;
		// Use smaller file as the outer
		// We create way too many 
		for(InputSplit vSplit : vSplits) {
			for(InputSplit uSplit : uSplits) {
				resultSplits[i] = new CompositeInputSplit(2);
				resultSplits[i].add(vSplit);
				resultSplits[i].add(uSplit);
				i++;
			}
		}
		
		return resultSplits;
	}
	
	private InputSplit[] getBlockLineSplits(JobConf conf, FileStatus status, int blockSize) throws IOException {
		// File?
		ArrayList<FileSplit> splits = new ArrayList<FileSplit>();
		
		for (org.apache.hadoop.mapreduce.lib.input.FileSplit split : 
			org.apache.hadoop.mapreduce.lib.input.
				NLineInputFormat.getSplitsForFile(status, conf, blockSize)) {
			splits.add(new FileSplit(split));
		}
			
		return splits.toArray(new FileSplit[splits.size()]);
	}

	public RecordReader<MatrixInputValueWritable, MatrixInputValueWritable> getRecordReader (InputSplit input, JobConf job, Reporter reporter) 
			throws IOException {

			return new MatrixInputRecordReader((CompositeInputSplit) input, job, reporter);
	}
}
