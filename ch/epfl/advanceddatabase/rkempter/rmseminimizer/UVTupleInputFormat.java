package ch.epfl.advanceddatabase.rkempter.rmseminimizer;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class UVTupleInputFormat extends FileInputFormat<IntWritable, MatrixInputValueWritable> {

	public RecordReader<IntWritable, MatrixInputValueWritable> getRecordReader(InputSplit input, JobConf job, Reporter reporter) 
			throws IOException {
		
		reporter.setStatus(input.toString());
		return new UVTupleRecordReader(job, (FileSplit) input);
	}
	
	// Files are equal to a split
	protected boolean isSplitable(FileSystem fs, Path filename) {
		return false; 
	}
}