package ch.epfl.advanceddatabase.rkempter.bloomfilter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import ch.epfl.advanceddatabase.rkempter.UVDecomposer;
import ch.epfl.advanceddatabase.rkempter.initialization.TupleValueWritable;
import ch.epfl.advanceddatabase.rkempter.rmseminimizer.MatrixInputValueWritable;
import ch.epfl.advanceddatabase.rkempter.rmseminimizer.MatrixUVValueWritable;

public class BloomFilterMapper extends MapReduceBase implements Mapper<IntWritable, MatrixInputValueWritable, IntWritable, MatrixInputValueWritable> {

	private int vectorSize;
	private int nbrHash;
	private float falsePosRate = (float) 0.1;
	private int user = UVDecomposer.NBR_USERS;
	
	public void configure(JobConf conf) {
		this.nbrHash = conf.getInt(UVDecomposer.NBR_HASH, 3);
		this.vectorSize = conf.getInt(UVDecomposer.VECTOR_SIZE, 91058);
	}
	
	// Identity Mapper
	public void map(IntWritable key, MatrixInputValueWritable value,
			OutputCollector<IntWritable, MatrixInputValueWritable> output, Reporter reporter) throws IOException {
		
		// create key
		int newRow = (int) key.get() / UVDecomposer.U_INPUT_BLOCK_SIZE;
		int newColumn = (int) value.getColumn() / UVDecomposer.BLOCK_SIZE;
		
		int columnNbr = (int) Math.ceil(UVDecomposer.NBR_MOVIES / UVDecomposer.BLOCK_SIZE);
		int newKey = newRow * columnNbr + newColumn;
		
		output.collect(new IntWritable(newKey), value);
	}

}
