package ch.epfl.advanceddatabase.rkempter.initialization;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Class containing a tuple from M, V or U matrix
 * 
 * @author rkempter
 *
 */
public class TupleValueWritable implements WritableComparable<TupleValueWritable>{
	private IntWritable row;
	private IntWritable column;
	private FloatWritable grade;
	
	public TupleValueWritable(int row, int column, float grade)
	{
		this.row = new IntWritable(row);
		this.column = new IntWritable(column);
		this.grade = new FloatWritable(grade);
	}

	public TupleValueWritable()
	{
		this.row = new IntWritable();
		this.column = new IntWritable();
		this.grade = new FloatWritable();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		row.write(out);
		column.write(out);
		grade.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		row.readFields(in);
		column.readFields(in);
		grade.readFields(in);
	}
	
	public float getGrade(){
		return grade.get();
	}
	
	public int getColumn() {
		return column.get();
	}
	
	public int getRow() {
		return row.get();
	}
	
	public void setGrade(float grade){
		this.grade.set(grade);
	}
	
	public void setColumn(int column) {
		this.column.set(column);
	}
	
	public void setRow(int row) {
		this.row.set(row);
	}
	
	public String toString() {
		return Integer.toString(row.get()) + ", "
				+ Integer.toString(column.get()) + ", "
				+ Float.toString(grade.get());
	}

	public int compareTo(TupleValueWritable other) {
		int cmp = row.compareTo(other.getRow());
		return cmp;
	}
}
