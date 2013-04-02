package ch.epfl.advanceddatabase.rkempter.rmseminimizer;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.join.CompositeInputSplit;

public class MatrixInputRecordReader<K1, V1, K2, V2> implements RecordReader<MatrixInputValueWritable, MatrixInputValueWritable> {
	
	private RecordReader<K1, V1> vRecordReader = null;
	private RecordReader<K2, V2> uRecordReader = null;
	
	private UVTupleInputFormat vFIF;
	
	// Helper variables
	private K1 vKey;
	private V1 vValue;
	private K2 uKey;
	private V2 uValue;
	
	private boolean allDone = false;
	
	public MatrixInputRecordReader(CompositeInputSplit split, JobConf conf, Reporter reporter) throws IOException {
		
		// Create vRecordReader
		UVTupleInputFormat uFIF = null;
		
		uFIF = new UVTupleInputFormat();
		this.uRecordReader = (RecordReader<K2, V2>) uFIF.getRecordReader(split.get(0), conf, reporter);
		
		// Create uRecordReader
		this.vFIF = new UVTupleInputFormat();
		this.uRecordReader = (RecordReader<K2, V2>) vFIF.getRecordReader(split.get(0), conf, reporter);
		
		// Create key/value pairs for parsing
		vKey = (K1) this.vRecordReader.createKey();
		vValue = (V1) this.vRecordReader.createValue();
		uKey = (K2) this.uRecordReader.createKey();
		uValue = (V2) this.uRecordReader.createValue();
		
	}
	
	/**
	 * This method takes a row from (subblock) matrix U and
	 * multiplies it with every column from the (subblock) matrix V.
	 * In order to go only once through the file, the elements of a V-row
	 * are saved in an array.
	 * 
	 * @param key
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public boolean next(MatrixInputValueWritable key, MatrixInputValueWritable value) throws IOException {
		
		if(uRecordReader.next(uKey, uValue) && vRecordReader.next(vKey, vValue)) {
			// Send the element from the u matrix as the key
			MatrixInputValueWritable uVal = (MatrixInputValueWritable) uValue;
			key.setValues(uVal.getType(), uVal.getRow(), uVal.getColumn(), uVal.getValue());
			// The element from the v matrix to the value
			MatrixInputValueWritable vVal = (MatrixInputValueWritable) vValue;
			value.setValues(vVal.getType(), vVal.getRow(), vVal.getColumn(), vVal.getValue());
		} else {
			allDone = true;
		}
		
		return !allDone;
	}

	@Override
	public MatrixInputValueWritable createKey() {
		return new MatrixInputValueWritable();
	}

	@Override
	public MatrixInputValueWritable createValue() {
		return new MatrixInputValueWritable();
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}
}
