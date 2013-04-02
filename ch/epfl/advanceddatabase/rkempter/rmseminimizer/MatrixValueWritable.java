package ch.epfl.advanceddatabase.rkempter.rmseminimizer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class MatrixValueWritable implements Writable{

	//([1...10] of u, uValue, rmse_result_x, rmse_result_k)>
	private int column;
	private float value;
	private int matrixType;
	private float result;
	
	public MatrixValueWritable(int column, float value, int matrixType, float result) {
		setValues(column, value, matrixType, result);
	}
	
	public MatrixValueWritable() {
		
	}
	
	public void setValues(int column, float value, int matrixType, float result) {
		this.column = column;
		this.value = value;
		this.result = result;
		this.matrixType = matrixType;
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

	public float getResult() {
		return result;
	}

	public void setResult(float result) {
		this.result = result;
	}

	public int getMatrixType() {
		return matrixType;
	}

	public void setMatrixType(int matrixType) {
		this.matrixType = matrixType;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		column = in.readInt();
		value = in.readFloat();
		matrixType = in.readInt();
		result = in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(column);
		out.writeFloat(value);
		out.writeInt(matrixType);
		out.writeFloat(result);
	}
	
	public String toString() {
		return Integer.toString(column) + ","
				+ Float.toString(value) + ","
				+ Integer.toString(matrixType) + ","
				+ Float.toString(result);
	}
	
}
