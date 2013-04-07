package ch.epfl.advanceddatabase.rkempter.rmseminimizer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * Element containing an element from the M-Matrix and the
 * corresponding elements 10 elements from the U & V matrices
 * 
 * @author rkempter
 *
 */
public class InputWritable implements Writable{
	private IntWritable index;
	private FloatWritable mElement;
	private FloatArrayWritable vElements;
	private FloatArrayWritable uElements;
	
	public InputWritable(int index, float mElement, Float[] uElements, Float[] vElements) {
		this.index = new IntWritable();
		this.mElement = new FloatWritable();
		this.vElements = new FloatArrayWritable();
		this.uElements = new FloatArrayWritable();
		setValues(index, mElement, uElements, vElements);
	}
	
	public InputWritable() {
		index = new IntWritable();
		mElement = new FloatWritable();
		vElements = new FloatArrayWritable();
		uElements = new FloatArrayWritable();
	}
	
	public void setValues(int index, float mElement, Float[] uElements, Float[] vElements) {
		this.index.set(index);
		this.mElement = new FloatWritable(mElement);
		FloatWritable[] vEls = new FloatWritable[10];
		FloatWritable[] uEls = new FloatWritable[10];
		
		for(int i = 0; i < uElements.length; i++) {
			uEls[i] = new FloatWritable(uElements[i]); 
		}
		
		for(int i = 0; i < vElements.length; i++) {
			vEls[i] = new FloatWritable(vElements[i]); 
		}
		
		this.vElements.set(vEls);
		this.uElements.set(uEls);
	}

	public int getIndex() {
		return index.get();
	}

	public void setIndex(int index) {
		this.index = new IntWritable(index);
	}

	public float getmElement() {
		return mElement.get();
	}

	public void setmElement(float mElement) {
		this.mElement = new FloatWritable(mElement);
	}

	public Float[] getvElements() {
		Float[] out = new Float[10];
		int i = 0;
		for(Writable elem : vElements.get()) {
			out[i] = ((FloatWritable) elem).get();
			i++;
		};
		return out;
	}

	public void setvElements(Float[] vElements) {
		FloatWritable[] vEls = new FloatWritable[10];
		for(int i = 0; i < vElements.length; i++) {
			vEls[i] = new FloatWritable(vElements[i]); 
		}
		
		this.vElements = new FloatArrayWritable();
		this.vElements.set(vEls);
	}

	public Float[] getuElements() {
		Float[] out = new Float[10];
		int i = 0;
		for(Writable elem : uElements.get()) {
			out[i] = ((FloatWritable) elem).get();
			i++;
		};
		return out;
	}
	
	public void setuElements(Float[] uElements) {
		FloatWritable[] uEls = new FloatWritable[10];
		for(int i = 0; i < uElements.length; i++) {
			uEls[i] = new FloatWritable(uElements[i]); 
		}
		
		this.uElements = new FloatArrayWritable();
		this.uElements.set(uEls);
	}

	public void readFields(DataInput in) throws IOException {
		index.readFields(in);
		mElement.readFields(in);
		vElements.readFields(in);
		uElements.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		index.write(out);
		mElement.write(out);
		vElements.write(out);
		uElements.write(out);
	}	
}

