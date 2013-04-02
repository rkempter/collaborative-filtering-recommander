package ch.epfl.advanceddatabase.rkempter.initialization;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class TupleValueWritable implements Writable{
	private int index;
	private float grade;
	
	public TupleValueWritable(int index, float grade)
	{
		this.index = index;
		this.grade = grade;
	}

	public TupleValueWritable()
	{
		this.index = 0;
		this.grade = 0;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(index);
		out.writeFloat(grade);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		index = in.readInt();
		grade = in.readFloat();
		// read datestring?
	}
	
	public float getGrade(){
		return grade;
	}
	
	public int getIndex() {
		return index;
	}
	
	public void setGrade(float grade){
		this.grade = grade;
	}
	
	public void setIndex(int index) {
		this.index = index;
	}
	
	public String toString() {
		return Integer.toString(index) + ", "
				+ Float.toString(grade);
	}
}
