package ch.epfl.advanceddatabase.rkempter.rmseminimizer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class MatrixUVValueWritable implements Writable{
	private float value_x;
	private float value_k;
	private int type;
	private int column;
	
	public MatrixUVValueWritable(int type, int column, float value_x, float value_k) {
		setValues(type, column, value_x, value_k);
	}
	
	public MatrixUVValueWritable() {
		
	}
	
	public void setValues(int type, int column, float value_x, float value_k) {
		this.type = type;
		this.column = column;
		this.value_x = value_x;
		this.value_k = value_k;
	}

	public void readFields(DataInput in) throws IOException {
		type = in.readInt();
		column = in.readInt();
		value_x = in.readFloat();
		value_k = in.readFloat();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(type);
		out.writeInt(column);
		out.writeFloat(value_x);
		out.writeFloat(value_k);
	}

	public float getValue_x() {
		return value_x;
	}

	public void setValue_x(float value_x) {
		this.value_x = value_x;
	}

	public float getValue_k() {
		return value_k;
	}

	public void setValue_k(float value_k) {
		this.value_k = value_k;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public int getColumn() {
		return column;
	}

	public void setColumn(int column) {
		this.column = column;
	}
}
