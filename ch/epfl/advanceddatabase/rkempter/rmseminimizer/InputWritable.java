package ch.epfl.advanceddatabase.rkempter.rmseminimizer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class InputWritable implements Writable{
	private IntWritable index;
	private FloatWritable mElement;
	private FloatArrayWritable vElements;
	private FloatArrayWritable uElements;
	
	public InputWritable(int index, float mElement, Float[] uElements, Float[] vElements) {
		setValues(index, mElement, uElements, vElements);
	}
	
	public void setValues(int index, float mElement, Float[] uElements, Float[] vElements) {
		this.index = new IntWritable(index);
		this.mElement = new FloatWritable(mElement);
		FloatWritable[] vEls = new FloatWritable[10];
		FloatWritable[] uEls = new FloatWritable[10];
		
		for(int i = 0; i < uElements.length; i++) {
			uEls[i] = new FloatWritable(uElements[i]); 
		}
		
		for(int i = 0; i < vElements.length; i++) {
			vEls[i] = new FloatWritable(vElements[i]); 
		}
		
		this.vElements = new FloatArrayWritable();
		this.vElements.set(vEls);
		this.uElements = new FloatArrayWritable();
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
		for(FloatWritable elem : (FloatWritable[]) vElements.get()) {
			out[i] = elem.get();
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
		for(FloatWritable elem : (FloatWritable[]) uElements.get()) {
			out[i] = elem.get();
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

	public void setuElements(FloatArrayWritable uElements) {
		this.uElements = uElements;
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

