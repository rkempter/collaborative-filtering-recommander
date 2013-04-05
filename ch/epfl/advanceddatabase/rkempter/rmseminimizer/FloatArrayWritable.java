package ch.epfl.advanceddatabase.rkempter.rmseminimizer;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;

public class FloatArrayWritable extends ArrayWritable {
	
	public FloatArrayWritable() { 
		super(FloatWritable.class); 
	} 
}
