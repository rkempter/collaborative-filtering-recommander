package ch.epfl.advanceddatabase.rkempter.initialization;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import ch.epfl.advanceddatabase.rkempter.IntPair;
import ch.epfl.advanceddatabase.rkempter.UVDecomposer;

public class InitializationMapper extends MapReduceBase implements Mapper<IntWritable, TupleValueWritable, IntPair, TupleValueWritable>
{
	public void map(IntWritable key, TupleValueWritable value, OutputCollector<IntPair, TupleValueWritable> output, Reporter reporter) throws IOException {
		
		// Compute the block-id
		int newRow = (int) Math.ceil((float) value.getRow() / UVDecomposer.U_INPUT_BLOCK_SIZE);
		int newColumn = (int) Math.ceil((float) value.getColumn() / UVDecomposer.V_INPUT_BLOCK_SIZE);
		int columnNbr = (int) Math.ceil((float) UVDecomposer.NBR_MOVIES / UVDecomposer.V_INPUT_BLOCK_SIZE);
		int newIndex = (newRow-1) * columnNbr + (newColumn-1);
		
		// Create the key with the block-id and the column, resp. row-id
		IntPair newKey = new IntPair(newIndex, key.get());
		// Send to reducer
		output.collect(newKey, value);
	}
}