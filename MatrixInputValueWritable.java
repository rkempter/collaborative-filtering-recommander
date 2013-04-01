import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class MatrixInputValueWritable implements WritableComparable{
	
	private char type;
	private int row;
	private int column;
	private float value;
	
	public MatrixInputValueWritable(char type, int row, int column, float value) {
		setValues(type, row, column, value);
	}
	
	public MatrixInputValueWritable() {
		
	}
	
	public void setValues(char type, int row, int column, float value) {
		this.type = type;
		this.row = row;
		this.column = column;
		this.value = value;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		type = in.readChar();
		row = in.readInt();
		column = in.readInt();
		value = in.readFloat();	
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeChar(type);
		out.writeInt(row);
		out.writeInt(column);
		out.writeFloat(value);
	}
	
	// Getters & Setters

	public char getType() {
		return type;
	}

	public void setType(char type) {
		this.type = type;
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

	public float getValue() {
		return value;
	}

	public void setValue(float value) {
		this.value = value;
	}

	@Override
	// Never used at reducer
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

}
