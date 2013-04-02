package ch.epfl.advanceddatabase.rkempter.initialization;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;


public class TupleValueInputFormat extends FileInputFormat<IntWritable, TupleValueWritable>{
	
	public RecordReader<IntWritable, TupleValueWritable> getRecordReader (InputSplit input, JobConf job, Reporter reporter) 
			throws IOException {

		    reporter.setStatus(input.toString());
		    return new InitTupleRecordReader(job, (FileSplit)input);
	}
}
