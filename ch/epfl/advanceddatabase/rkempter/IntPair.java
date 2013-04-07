package ch.epfl.advanceddatabase.rkempter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/*
 * This class is used as Key for output tuples. It contains an int indicating
 * the block, in which the element lies and another int indicating the row (in case of U / M)
 * or the column (in case of V).
 */
public class IntPair implements WritableComparable<IntPair>{
	private int block;
	private int index;
	
	public IntPair() {
	}
	
	public IntPair(int block, int index) {
		set(block, index);
	}
	
	public void set(int first, int second) {
		this.block = first;
		this.index = second;
	}
	
	public int getIndex() {
		return index;
	}
	
	public int getBlock() {
		return block;
	}
	
	// Writable
	public void write(DataOutput out) throws IOException {
		out.writeInt(block);
		out.writeInt(index);
	}
	
	// Writable
	public void readFields(DataInput in) throws IOException {
		block = in.readInt();
		index = in.readInt();
	}
	
	public int hashCode() {
		return block * 163 + index;
	}
	
	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(IntPair.class);
		}
		
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
		
	}
	
	static {                                        
		// register this comparator
		WritableComparator.define(IntPair.class, new Comparator());
	}
	
	 public static int compare(int a, int b) {
		 return (a < b ? -1 : (a == b ? 0 : 1));
	 }
	
	 public boolean equals(Object o) {
		 if (o instanceof IntPair) {
		      IntPair ip = (IntPair) o;
		      return block == ip.block && index == ip.index;
		 }
		 return false;
	 }
	
	public String toString() {
		return block + "," + index;
	}
	
	public int compareTo(IntPair ip) {
	    int cmp = compare(block, ip.block);
	    if (cmp != 0) {
	      return cmp;
	    }
	    return compare(index, ip.index);
	}
	
	
}


