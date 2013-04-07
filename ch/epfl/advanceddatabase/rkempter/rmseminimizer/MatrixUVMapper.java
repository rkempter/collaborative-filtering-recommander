package ch.epfl.advanceddatabase.rkempter.rmseminimizer;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import ch.epfl.advanceddatabase.rkempter.IntPair;
import ch.epfl.advanceddatabase.rkempter.UVDecomposer;


public class MatrixUVMapper extends MapReduceBase implements Mapper<IntWritable, InputWritable, IntPair, InputWritable> {
	
	private int matrix = 1;
	
	public void configure(JobConf conf) {
		matrix = conf.getInt(UVDecomposer.MATRIX_TYPE, 1);
	}
	
	public void map(IntWritable key, InputWritable element,
			OutputCollector<IntPair, InputWritable> output, Reporter reporter) throws IOException {
		
		int newRow = (int) Math.ceil((float) key.get() / UVDecomposer.U_INPUT_BLOCK_SIZE);
		int newColumn = (int) Math.ceil((float) element.getIndex() / UVDecomposer.V_INPUT_BLOCK_SIZE);
		
		if(matrix == UVDecomposer.MATRIX_V) {
			newRow = (int) Math.ceil((float) element.getIndex() / UVDecomposer.U_INPUT_BLOCK_SIZE);
			newColumn = (int) Math.ceil((float) key.get() / UVDecomposer.V_INPUT_BLOCK_SIZE);
		}
		
		int columnNbr = (int) Math.ceil((float) UVDecomposer.NBR_MOVIES / UVDecomposer.V_INPUT_BLOCK_SIZE);
		int newIndex = (newRow-1) * columnNbr + (newColumn-1);
		
		IntPair newKey = new IntPair(newIndex, key.get());
		
		output.collect(newKey, element);
	}
}