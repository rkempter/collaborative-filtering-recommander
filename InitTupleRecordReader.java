import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;


class InitTupleRecordReader implements RecordReader<IntWritable, TupleValue> {

  private LineRecordReader lineReader;
  private LongWritable lineKey;
  private Text lineValue;

  public InitTupleRecordReader(JobConf job, FileSplit split) throws IOException {
    lineReader = new LineRecordReader(job, split);

    lineKey = lineReader.createKey();
    lineValue = lineReader.createValue();
  }

  public boolean next(IntWritable key, TupleValue value) throws IOException {
    // get the next line
    if (!lineReader.next(lineKey, lineValue)) {
      return false;
    }

    // parse the lineValue which is in the format:
    // objName, x, y, z
    String [] pieces = lineValue.toString().split(",");
    if (pieces.length != 4) {
      throw new IOException("Invalid record received");
    }

    // try to parse floating point components of value
    int index;
    float grade;
    try {
      index = Integer.parseInt(pieces[1].trim());
      grade = Float.parseFloat(pieces[2].trim());
      String date = pieces[3].trim();
    } catch (NumberFormatException nfe) {
      throw new IOException("Error parsing values in record");
    }

    key.set(Integer.parseInt(pieces[0].trim())); // userID is the output key.

    value.setIndex(index);
    value.setGrade(grade);

    return true;
  }

  public IntWritable createKey() {
    return new IntWritable();
  }

  public TupleValue createValue() {
    return new TupleValue();
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