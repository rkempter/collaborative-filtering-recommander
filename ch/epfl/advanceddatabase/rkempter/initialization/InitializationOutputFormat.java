package ch.epfl.advanceddatabase.rkempter.initialization;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Progressable;

import ch.epfl.advanceddatabase.rkempter.UVDecomposer;
import ch.epfl.advanceddatabase.rkempter.rmseminimizer.MatrixUVOutputFormat;

/**
 * Depending 
 */
public class InitializationOutputFormat<K, V> extends MultipleTextOutputFormat<IntWritable, TupleValueWritable> {
	
	/**
	 * Generate the output path for the tuple (M-records are saved in output/M/, U-records in output/U_0/)
	 */
	protected String generateFileNameForKeyValue(IntWritable key, TupleValueWritable value, String name) {
		int newKey;
		if(key.get() == UVDecomposer.MATRIX_M) {
			int newRow = (int) Math.ceil((float) value.getRow() / UVDecomposer.U_INPUT_BLOCK_SIZE);
			int newColumn = (int) Math.ceil((float) value.getColumn() / UVDecomposer.V_INPUT_BLOCK_SIZE);
			int columnNbr = (int) Math.ceil((float) UVDecomposer.NBR_MOVIES / UVDecomposer.V_INPUT_BLOCK_SIZE);
			newKey = (newRow-1) * columnNbr + (newColumn-1);
		} else {
			newKey = (int) Math.ceil((float) value.getRow() / UVDecomposer.U_INPUT_BLOCK_SIZE);
		}
		String outputPath;
		if(key.get() == UVDecomposer.MATRIX_M) {
			outputPath = String.format("M/part-%05d", newKey);
		} else {
			outputPath = String.format("U_0/part-%05d", newKey);
		}
		
		return outputPath;
	}
	
	protected static class InitWriter<K, V> implements RecordWriter<IntWritable, TupleValueWritable> {
		
		private DataOutputStream out;
		
		public InitWriter(DataOutputStream out) throws IOException {
			this.out = out;
		}
		
		public synchronized void write(IntWritable key, TupleValueWritable value) throws IOException {
			String output;
			if(key.get() == UVDecomposer.MATRIX_U) {
				output = String.format("<%s,%d,%d,%f>\n", "U", value.getRow(), value.getColumn(), value.getGrade());
			} else {
				output = String.format("%d, %d, %f\n", value.getRow(), value.getColumn(), value.getGrade());
			}
		    out.writeBytes(output);
		}

		public synchronized void close(Reporter reporter) throws IOException {
			out.close();
		}
		
	}
	
	public RecordWriter<IntWritable, TupleValueWritable> getBaseRecordWriter(FileSystem ignored, JobConf conf,
			String name, Progressable progress) throws IOException {
		Path file = MatrixUVOutputFormat.getTaskOutputPath(conf, name);
		FileSystem fs = file.getFileSystem(conf);
		boolean overwrite = true;
		FSDataOutputStream fileOut = fs.create(file, overwrite);
		return new InitWriter<IntWritable, TupleValueWritable>(fileOut);
	}
}
