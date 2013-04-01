import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class MatrixValueWritable implements Writable{

	//([1...10] of u, uValue, rmse_result_x, rmse_result_k)>
	private int column;
	private float value;
	private float result_x;
	private float result_k;
	
	public MatrixValueWritable(int column, float value, float result_x, float result_k) {
		setValues(column, value, result_x, result_k);
	}
	
	public MatrixValueWritable() {
		
	}
	
	public void setValues(int column, float value, float result_x, float result_k) {
		this.column = column;
		this.value = value;
		this.result_x = result_x;
		this.result_k = result_k;
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

	public float getResult_x() {
		return result_x;
	}

	public void setResult_x(float result_x) {
		this.result_x = result_x;
	}

	public float getResult_k() {
		return result_k;
	}

	public void setResult_k(float result_k) {
		this.result_k = result_k;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		column = in.readInt();
		value = in.readFloat();
		result_x = in.readFloat();
		result_k = in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(column);
		out.writeFloat(value);
		out.writeFloat(result_x);
		out.writeFloat(result_k);
	}
	
	public String toString() {
		return Integer.toString(column) + ","
				+ Float.toString(value) + ","
				+ Float.toString(result_x) + ","
				+ Float.toString(result_k);
	}
	
}
