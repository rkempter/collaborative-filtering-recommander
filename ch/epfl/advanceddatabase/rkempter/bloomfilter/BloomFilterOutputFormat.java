package ch.epfl.advanceddatabase.rkempter.bloomfilter;

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
import org.apache.hadoop.util.bloom.BloomFilter;


public class BloomFilterOutputFormat<K, V> extends FileOutputFormat<IntWritable, BloomFilter> {

	protected static class BloomFilterWriter<K, V> implements RecordWriter<IntWritable, BloomFilter> {
		
		private FSDataOutputStream out;
		
		public BloomFilterWriter(FSDataOutputStream out) throws IOException {
			this.out = out;
		}
		
		public synchronized void write(IntWritable key, BloomFilter filter) throws IOException {
		    filter.write(out);
		}

		public synchronized void close(Reporter reporter) throws IOException {
			out.close();
		}
	}
	
	public RecordWriter<IntWritable, BloomFilter> getRecordWriter(FileSystem ignored, JobConf conf,
			String name, Progressable progress) throws IOException {
		Path file = FileOutputFormat.getTaskOutputPath(conf, name);
		FileSystem fs = file.getFileSystem(conf);
		FSDataOutputStream fileOut = fs.create(file, progress);
		return new BloomFilterWriter<IntWritable, BloomFilter>(fileOut);
	}
}