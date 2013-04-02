package ch.epfl.advanceddatabase.rkempter.rmseminimizer;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;


public class MatrixUVOutputFormat<K, V> extends FileOutputFormat<IntWritable, MatrixInputValueWritable> {
	
	protected static class MatrixUVElementWriter<K, V> implements RecordWriter<IntWritable, MatrixInputValueWritable> {
		private static final String utf8 = "UTF-8";
		
		private DataOutputStream out;
		
		public MatrixUVElementWriter(DataOutputStream out) throws IOException {
			this.out = out;
		}
		
		public synchronized void write(IntWritable key, MatrixInputValueWritable value) throws IOException {
		    String output = String.format("<%c, %d, %d, %f>\n", value.getType(), key.get(), value.getColumn(), value.getValue());
		    out.writeBytes(output);
		}

		public synchronized void close(Reporter reporter) throws IOException {
			out.close();
		}
	}
	
	
	public RecordWriter<IntWritable, MatrixInputValueWritable> getRecordWriter(FileSystem ignored, JobConf conf,
			String name, Progressable progress) throws IOException {
		Path file = FileOutputFormat.getTaskOutputPath(conf, name);
		FileSystem fs = file.getFileSystem(conf);
		FSDataOutputStream fileOut = fs.create(file, progress);
		return new MatrixUVElementWriter<IntWritable, MatrixInputValueWritable>(fileOut);
	}

}
