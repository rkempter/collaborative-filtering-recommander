package ch.epfl.advanceddatabase.rkempter.rmseminimizer;

import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.join.CompositeInputSplit;

import ch.epfl.advanceddatabase.rkempter.UVDecomposer;

public class MatrixInputRecordReader<K1, V1, K2, V2> implements RecordReader<MatrixInputValueWritable, MatrixInputValueWritable> {
	
	private RecordReader<K1, V1> vRecordReader = null;
	private RecordReader<K2, V2> uRecordReader = null;
	
	private UVTupleInputFormat vFIF;
	private JobConf vConf;
	private Reporter vReporter;
	private InputSplit vSplit;
	
	// Helper variables
	private K1 vKey;
	private V1 vValue;
	private K2 uKey;
	private V2 uValue;
	
	private MatrixInputValueWritable[] uValues = new MatrixInputValueWritable[UVDecomposer.D_DIMENSION];
	private MatrixInputValueWritable[] vValues = new MatrixInputValueWritable[UVDecomposer.D_DIMENSION*UVDecomposer.BLOCK_SIZE];
	private boolean uRead = false, vRead = false;
	private int uIndex = 0, vIndex = 0;
	private int uRowCounter = 1;
	
	public MatrixInputRecordReader(CompositeInputSplit split, JobConf conf, Reporter reporter) throws IOException {
		
		// Create vRecordReader
		UVTupleInputFormat uFIF = null;
		this.vConf = conf;
		this.vSplit = split.get(0);
		this.vReporter = reporter;
		
		uFIF = new UVTupleInputFormat();
		uRecordReader = (RecordReader<K2, V2>) uFIF.getRecordReader(split.get(1), conf, reporter);
		
		// Create uRecordReader
		vFIF = new UVTupleInputFormat();
		vRecordReader = (RecordReader<K1, V1>) vFIF.getRecordReader(split.get(0), conf, reporter);
		
		// Create key/value pairs for parsing
		uKey = (K2) this.uRecordReader.createKey();
		uValue = (V2) this.uRecordReader.createValue();
		vKey = (K1) this.vRecordReader.createKey();
		vValue = (V1) this.vRecordReader.createValue();
		
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
		
		if(uRowCounter == UVDecomposer.U_INPUT_BLOCK_SIZE && vIndex == (UVDecomposer.BLOCK_SIZE * UVDecomposer.D_DIMENSION - 1)) {
			key.setValues(uValues[uIndex].getType(), uValues[uIndex].getRow(), uValues[uIndex].getColumn(), uValues[uIndex].getValue());
			value.setValues(vValues[vIndex].getType(), vValues[vIndex].getRow(), vValues[vIndex].getColumn(), vValues[vIndex].getValue());
			return false;
		}
		
		if(!uRead) {
			if(uRecordReader.next(uKey, uValue)) {
				MatrixInputValueWritable uVal = (MatrixInputValueWritable) uValue;
				uValues[uIndex] = new MatrixInputValueWritable(uVal.getType(), uVal.getRow(), uVal.getColumn(), uVal.getValue());
			}
		}
		// Output value
		key.setValues(uValues[uIndex].getType(), uValues[uIndex].getRow(), uValues[uIndex].getColumn(), uValues[uIndex].getValue());
		uIndex++;
		if(uIndex >= UVDecomposer.D_DIMENSION) {
			uIndex = 0;
			uRead = true;
		}
		
		if(!vRead) {
			if(vRecordReader.next(vKey, vValue)) {
				MatrixInputValueWritable vVal = (MatrixInputValueWritable) vValue;
				vValues[vIndex] = new MatrixInputValueWritable(vVal.getType(), vVal.getRow(), vVal.getColumn(), vVal.getValue());
			}
		}
		value.setValues(vValues[vIndex].getType(), vValues[vIndex].getRow(), vValues[vIndex].getColumn(), vValues[vIndex].getValue());
		vIndex++;
		if(vIndex >= UVDecomposer.BLOCK_SIZE * UVDecomposer.D_DIMENSION) {
			vIndex = 0;
			uRead = false;
			uRowCounter++;
		}
	
		return true;
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
