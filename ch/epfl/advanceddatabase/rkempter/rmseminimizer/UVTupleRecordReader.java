package ch.epfl.advanceddatabase.rkempter.rmseminimizer;

import java.io.IOException;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

class UVTupleRecordReader implements RecordReader<IntWritable, MatrixInputValueWritable> {
	private LineRecordReader lineReader;
	private LongWritable lineKey;
	private Text lineValue;
	
	public UVTupleRecordReader(JobConf job, FileSplit split) throws IOException {
		lineReader = new LineRecordReader(job, split);
		
		lineKey = lineReader.createKey();
		lineValue = lineReader.createValue();
	}
	
	public boolean next(IntWritable key, MatrixInputValueWritable tupleValue) throws IOException {
		if(!lineReader.next(lineKey, lineValue)) {
			return false;
		}
		
		String stringToMatch = lineValue.toString().trim();
		String regexpPattern = ".*<([UV]),([0-9]+),([0-9]+),(-?[0-9].[0-9]+)>.*";
		Matcher matcher = Pattern.compile(regexpPattern).matcher(stringToMatch);

		if(matcher.matches()) {
			if(matcher.groupCount() != 4) {
				throw new IOException("Invalid record received");
			}
		
			int row;
			int column;
			float value;
			String type = matcher.group(1).trim();
			
			try{
				row = Integer.parseInt(matcher.group(2).trim());
				column = Integer.parseInt(matcher.group(3).trim());
				value = Float.parseFloat(matcher.group(4).trim());
			} catch(NumberFormatException nfe) {
				throw new IOException("Error parsing values in record");
			}
			
			key.set(row);
			if(type == "U") {
				tupleValue.setValues('U', row, column, value);
			} else {
				tupleValue.setValues('V', row, column, value);
			}
			return true;
		}
		
		return false;
	}

	public void close() throws IOException {
		lineReader.close();
	}

	public IntWritable createKey() {
		// TODO Auto-generated method stub
		return new IntWritable();
	}

	public MatrixInputValueWritable createValue() {
		// TODO Auto-generated method stub
		return new MatrixInputValueWritable();
	}

	public long getPos() throws IOException {
		return lineReader.getPos();
	}

	public float getProgress() throws IOException {
		return lineReader.getProgress();
	}
}
