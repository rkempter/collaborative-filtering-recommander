import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;


public class MatrixUVOutputFormat<MatrixUVKeyWritable, MatrixUVValueWritable> extends FileOutputFormat<MatrixUVKeyWritable, MatrixUVValueWritable> {
	
	
	protected static class MatrixUVElementWriter<MatrixUVKeyWritable, MatrixUVValueWritable> implements RecordWriter<MatrixUVKeyWritable, MatrixUVValueWritable> {
		private DataOutputStream out;
		
		public MatrixUVElementWriter(DataOutputStream out) throws IOException {
			this.out = out;
		}
		
		public synchronized void write(MatrixUVKeyWritable key, MatrixUVValueWritable value) throws IOException {

			boolean nullKey = key == null || key instanceof NullWritable;
		    boolean nullValue = value == null || value instanceof NullWritable;

		    if (nullKey && nullValue) {
		    	return;
		    }
		    
		    String output = String.format("<%s, %d, %d, %f>\n", key.getType(), key.getRow(), key.getColumn(), value.getValue());
		    out.writeChars(output);
		}

		public synchronized void close(Reporter reporter) throws IOException {
			out.close();
		}
	}
	
	
	public RecordWriter<MatrixUVKeyWritable, MatrixUVValueWritable> getRecordWriter(FileSystem ignored, JobConf conf,
			String name, Progressable progress) throws IOException {
		Path file = FileOutputFormat.getTaskOutputPath(conf, name);
		FileSystem fs = file.getFileSystem(conf);
		FSDataOutputStream fileOut = fs.create(file, progress);
		return new MatrixUVElementWriter<MatrixUVKeyWritable, MatrixUVValueWritable>(fileOut);
	}

}
