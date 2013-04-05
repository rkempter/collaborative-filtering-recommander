package ch.epfl.advanceddatabase.rkempter.initialization;

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


public class InitializationOutputFormat<K, V> extends MultipleTextOutputFormat<NullWritable, TupleValueWritable> {
	
	protected String generateFileNameForKeyValue(NullWritable key, TupleValueWritable value, String name) {
		int newRow = (int) (value.getRow()-1) / UVDecomposer.U_INPUT_BLOCK_SIZE;
		System.out.println("Row: "+value.getRow());
		int newColumn = (int) (value.getColumn()-1) / UVDecomposer.BLOCK_SIZE;
		int columnNbr = (int) Math.ceil(UVDecomposer.NBR_MOVIES / UVDecomposer.BLOCK_SIZE);
		int newKey = newRow * columnNbr + newColumn;
		
		return Integer.toString(newKey); 
	}
}
