package ch.epfl.advanceddatabase.rkempter;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import ch.epfl.advanceddatabase.rkempter.initialization.TupleValueWritable;


/**
 * Compare two elements, if their keys are the same (sorting)
 * 
 * @author rkempter
 *
 */
public class KeyComparator extends WritableComparator {
	
	protected KeyComparator() {
		super(IntPair.class, true);
	}

	public int compare(WritableComparable w1, WritableComparable w2) {
		IntPair ip1 = (IntPair) w1;
		IntPair ip2 = (IntPair) w2;
		
		int cmp = IntPair.compare(ip1.getBlock(), ip2.getBlock());
		// If they are not in the same partition
		if (cmp != 0) {
			return cmp; 
		}
		return IntPair.compare(ip1.getIndex(), ip2.getIndex());
	}
}