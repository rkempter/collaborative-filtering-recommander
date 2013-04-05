package ch.epfl.advanceddatabase.rkempter.bloomfilter;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.io.NullWritable;

import ch.epfl.advanceddatabase.rkempter.UVDecomposer;

public class BloomFilterReducer extends MapReduceBase implements Reducer<IntWritable, MatrixInputValueWritable, IntWritable, BloomFilter> {

	public void reduce(IntWritable key, Iterator<BloomFilter> values,
			OutputCollector<IntWritable, BloomFilter> output, Reporter reporter) throws IOException {
		// replace KeyType with the real type of your key
		
		BloomFilter filter = new BloomFilter(vectorSize, nbrHash, Hash.MURMUR_HASH);
		int column = value.getIndex();
		int row = key.get();
		
		int index = ((row-1) * UVDecomposer.NBR_MOVIES + column);
		byte[] keyIndex = Integer.toString(index).getBytes();
		Key newKey = new Key(keyIndex);
		filter.add(newKey);
		
		IntWritable outKey = new IntWritable(1);
		
		output.collect(outKey, filter);
		
		
		boolean init = false;
		BloomFilter filter = new BloomFilter(910580, 3, Hash.MURMUR_HASH);
		while (values.hasNext()) {
			filter.or(values.next());
		}
			
		if(filter.membershipTest(new Key(Integer.toString(55299).getBytes()))) {
			System.out.println("55299 inside");
		}
		System.out.println("Filter at end: "+filter.toString());
		output.collect(key, filter);
	}

}
