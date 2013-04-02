package ch.epfl.advanceddatabase.rkempter.rmseminimizer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class MatrixUVKeyWritable implements WritableComparable {
	private int row;
	
	public MatrixUVKeyWritable(int row, int column, char type) {
		setValues(row, column, type);
	}
	
	public MatrixUVKeyWritable() {
		
	}
	
	public void setValues(int row, int column, char type) {
		this.row = row;
	}
	
	public void readFields(DataInput in) throws IOException {
		row = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(row);
	}

	public int compareTo(Object that) {
		if(that instanceof MatrixUVKeyWritable) {
			MatrixUVKeyWritable other = (MatrixUVKeyWritable) that;
			if(row == other.getRow()) {
				return 0;
			} else if(row > other.getRow()) {
				return 1;
			} else if(row < other.getRow()) {
				return -1;
			}
		}
		return 1;
	}

	public int getRow() {
		return row;
	}

	public void setRow(int row) {
		this.row = row;
	}
}
