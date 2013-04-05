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
import ch.epfl.advanceddatabase.rkempter.rmseminimizer.MatrixUVValueWritable;

public class BloomFilterMapper extends MapReduceBase implements Mapper<IntWritable, TupleValueWritable, IntWritable, BloomFilter> {

	private int vectorSize;
	private int nbrHash;
	private float falsePosRate = (float) 0.1;
	private int user = UVDecomposer.NBR_USERS;
	
	public void configure(JobConf conf) {
		this.nbrHash = conf.getInt(UVDecomposer.NBR_HASH, 3);
		this.vectorSize = conf.getInt(UVDecomposer.VECTOR_SIZE, 91058);
	}
	
	// Identity Mapper
	public void map(IntWritable key, TupleValueWritable value,
			OutputCollector<IntWritable, BloomFilter> output, Reporter reporter) throws IOException {
		
		BloomFilter filter = new BloomFilter(vectorSize, nbrHash, Hash.MURMUR_HASH);
		int column = value.getIndex();
		int row = key.get();
		
		int index = ((row-1) * UVDecomposer.NBR_MOVIES + column);
		byte[] keyIndex = Integer.toString(index).getBytes();
		Key newKey = new Key(keyIndex);
		filter.add(newKey);
		
		IntWritable outKey = new IntWritable(1);
		
		output.collect(outKey, filter);
		 
	}

}
