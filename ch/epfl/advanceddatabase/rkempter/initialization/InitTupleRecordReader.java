package ch.epfl.advanceddatabase.rkempter.initialization;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;


class InitTupleRecordReader implements RecordReader<IntWritable, TupleValueWritable> {

  private LineRecordReader lineReader;
  private LongWritable lineKey;
  private Text lineValue;

  public InitTupleRecordReader(JobConf job, FileSplit split) throws IOException {
    lineReader = new LineRecordReader(job, split);

    lineKey = lineReader.createKey();
    lineValue = lineReader.createValue();
  }

  public boolean next(IntWritable key, TupleValueWritable value) throws IOException {
    // get the next line
    if (!lineReader.next(lineKey, lineValue)) {
      return false;
    }

    // parse the lineValue which is in the format:
    // objName, x, y, z
    String [] pieces = lineValue.toString().split(",");
    if (pieces.length < 3) {
      throw new IOException("Invalid record received");
    }

    // try to parse floating point components of value
    int row, column;
    float grade;
    try {
    	row = Integer.parseInt(pieces[0].trim());
    	column = Integer.parseInt(pieces[1].trim());
    	grade = Float.parseFloat(pieces[2].trim());
    } catch (NumberFormatException nfe) {
      throw new IOException("Error parsing values in record");
    }

    key.set(Integer.parseInt(pieces[0].trim())); // userID is the output key.

    value.setRow(row);
    value.setColumn(column);
    value.setGrade(grade);

    return true;
  }

  public IntWritable createKey() {
    return new IntWritable();
  }

  public TupleValueWritable createValue() {
    return new TupleValueWritable();
  }

  public long getPos() throws IOException {
    return lineReader.getPos();
  }

  public void close() throws IOException {
    lineReader.close();
  }

  public float getProgress() throws IOException {
    return lineReader.getProgress();
  }
}