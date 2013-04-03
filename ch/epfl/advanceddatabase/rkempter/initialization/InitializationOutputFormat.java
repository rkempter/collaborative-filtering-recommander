package ch.epfl.advanceddatabase.rkempter.initialization;

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


public class InitializationOutputFormat<K, V> extends FileOutputFormat<IntWritable, TupleValueWritable> {
	
	protected static class InitializationElementWriter<K, V> implements RecordWriter<IntWritable, TupleValueWritable> {
		
		private DataOutputStream out;
		
		public InitializationElementWriter(DataOutputStream out) throws IOException {
			this.out = out;
		}
		
		public synchronized void write(IntWritable key, TupleValueWritable value) throws IOException {
		    String output = String.format("%d,%d,%f\n", key.get(), value.getIndex(), value.getGrade());
		    out.writeBytes(output);
		}

		public synchronized void close(Reporter reporter) throws IOException {
			out.close();
		}
	}
	
	
	public RecordWriter<IntWritable, TupleValueWritable> getRecordWriter(FileSystem ignored, JobConf conf,
			String name, Progressable progress) throws IOException {
		Path file = FileOutputFormat.getTaskOutputPath(conf, name);
		FileSystem fs = file.getFileSystem(conf);
		FSDataOutputStream fileOut = fs.create(file, progress);
		return new InitializationElementWriter<IntWritable, TupleValueWritable>(fileOut);
	}

}
