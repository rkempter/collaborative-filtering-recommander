package ch.epfl.advanceddatabase.rkempter.initialization;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class TupleValueWritable implements Writable{
	private int row;
	private int column;
	private float grade;
	
	public TupleValueWritable(int row, int column, float grade)
	{
		this.row = row;
		this.column = column;
		this.grade = grade;
	}

	public TupleValueWritable()
	{
		this.row = 0;
		this.column = 0;
		this.grade = 0;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(row);
		out.writeInt(column);
		out.writeFloat(grade);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		row = in.readInt();
		column = in.readInt();
		grade = in.readFloat();
		// read datestring?
	}
	
	public float getGrade(){
		return grade;
	}
	
	public int getColumn() {
		return column;
	}
	
	public int getRow() {
		return row;
	}
	
	public void setGrade(float grade){
		this.grade = grade;
	}
	
	public void setColumn(int column) {
		this.column = column;
	}
	
	public void setRow(int row) {
		this.row = row;
	}
	
	public String toString() {
		return Integer.toString(row) + ", "
				+ Integer.toString(column) + ", "
				+ Float.toString(grade);
	}
}
