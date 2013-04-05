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
import org.apache.hadoop.io.NullWritable;

public class BloomFilterReducer extends MapReduceBase implements Reducer<IntWritable,  BloomFilter, IntWritable, BloomFilter> {

	public void reduce(IntWritable key, Iterator<BloomFilter> values,
			OutputCollector<IntWritable, BloomFilter> output, Reporter reporter) throws IOException {
		// replace KeyType with the real type of your key
		boolean init = false;
		BloomFilter filter = new BloomFilter();
		while (values.hasNext()) {
			if(!init) {
				init = true;
				filter = (BloomFilter) values.next();
			} else {
				BloomFilter newFilter = (BloomFilter) values.next();
				filter.or(newFilter);
			}
			System.out.println("Filter: "+filter.toString());
		}
		
		if(filter.membershipTest(new Key(Integer.toString(55299).getBytes()))) {
			System.out.println("55299 inside");
		}
		
		output.collect(key, filter);
	}

}
