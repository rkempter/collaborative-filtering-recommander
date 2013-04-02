import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class MatrixUVValueWritable implements Writable{
	private float value;
	
	public MatrixUVValueWritable(float value) {
		this.value = value;
	}
	
	public MatrixUVValueWritable() {
		
	}
	
	public void setValue(float value) {
		this.value = value;
	}
	
	public float getValue() {
		return value;
	}

	public void readFields(DataInput in) throws IOException {
		value = in.readFloat();
	}

	public void write(DataOutput out) throws IOException {
		out.writeFloat(value);
	}
}
