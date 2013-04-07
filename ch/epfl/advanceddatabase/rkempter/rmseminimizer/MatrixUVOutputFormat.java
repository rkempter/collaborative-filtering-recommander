package ch.epfl.advanceddatabase.rkempter.rmseminimizer;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Progressable;

import ch.epfl.advanceddatabase.rkempter.UVDecomposer;
import ch.epfl.advanceddatabase.rkempter.initialization.TupleValueWritable;


public class MatrixUVOutputFormat<K, V> extends MultipleTextOutputFormat<IntWritable, MatrixInputValueWritable> {
	
	protected static class MatrixUVElementWriter<K, V> implements RecordWriter<IntWritable, MatrixInputValueWritable> {
		
		private DataOutputStream out;
		
		public MatrixUVElementWriter(DataOutputStream out) throws IOException {
			this.out = out;
		}
		
		public synchronized void write(IntWritable key, MatrixInputValueWritable value) throws IOException {
		    String output = String.format("<%c,%d,%d,%f>\n", value.getType(), value.getRow(), value.getColumn(), value.getValue());
		    out.writeBytes(output);
		}

		public synchronized void close(Reporter reporter) throws IOException {
			out.close();
		}
		
	}
	
	/**
	 * Create key for the U, V records, depending on their block
	 */
	protected String generateFileNameForKeyValue(IntWritable key, MatrixInputValueWritable value, String name) {		
		int newKey;
		if(value.getType() == 'U') {
			newKey = key.get() / (UVDecomposer.U_INPUT_BLOCK_SIZE*UVDecomposer.D_DIMENSION);
		} else {
			newKey = key.get() / (UVDecomposer.V_INPUT_BLOCK_SIZE*UVDecomposer.D_DIMENSION);
		}
		
		return String.format("part-%05d", newKey);
	}
	
	public RecordWriter<IntWritable, MatrixInputValueWritable> getBaseRecordWriter(FileSystem ignored, JobConf conf,
			String name, Progressable progress) throws IOException {
		Path file = MatrixUVOutputFormat.getTaskOutputPath(conf, name);
		FileSystem fs = file.getFileSystem(conf);
		boolean overwrite = true;
		FSDataOutputStream fileOut = fs.create(file, overwrite);
		return new MatrixUVElementWriter<IntWritable, MatrixInputValueWritable>(fileOut);
	}

}
