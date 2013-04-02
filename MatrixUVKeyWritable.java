import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class MatrixUVKeyWritable implements WritableComparable{
	private int row;
	private int column;
	private char type;
	
	public MatrixUVKeyWritable(int row, int column, char type) {
		setValues(row, column, type);
	}
	
	public MatrixUVKeyWritable() {
		
	}
	
	public void setValues(int row, int column, char type) {
		this.row = row;
		this.column = column;
		this.type = type;
	}
	
	public void readFields(DataInput in) throws IOException {
		row = in.readInt();
		column = in.readInt();
		type = in.readChar();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(row);
		out.writeInt(column);
		out.writeChar(type);
	}

	public int compareTo(Object that) {
		if(that instanceof MatrixUVKeyWritable) {
			MatrixUVKeyWritable other = (MatrixUVKeyWritable) that;
			if(row == other.getRow() && column == other.getColumn()) {
				return 0;
			}
			if(type == 'V') {
				if(row > other.getRow()) {
					return 1;
				} else {
					return -1;
				}
			} else if(type == 'U') {
				if(column > other.getColumn()) {
					return 1;
				} else {
					return -1;
				}
			}
			
			return 0;
		} else {
			return 1;
		}
	}

	public int getRow() {
		return row;
	}

	public void setRow(int row) {
		this.row = row;
	}

	public int getColumn() {
		return column;
	}

	public void setColumn(int column) {
		this.column = column;
	}

	public char getType() {
		return type;
	}

	public void setType(char type) {
		this.type = type;
	}
	
}
