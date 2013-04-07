package ch.epfl.advanceddatabase.rkempter.rmseminimizer;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.join.CompositeInputSplit;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import ch.epfl.advanceddatabase.rkempter.UVDecomposer;
import ch.epfl.advanceddatabase.rkempter.initialization.TupleValueInputFormat;
import ch.epfl.advanceddatabase.rkempter.initialization.TupleValueWritable;

public class MatrixInputRecordReader<K1, V1, K2, V2, K3, V3> implements RecordReader<IntWritable, InputWritable> {
	
	private RecordReader<K1, V1> mRecordReader = null;
	private RecordReader<K2, V2> uRecordReader = null;
	private RecordReader<K3, V3> vRecordReader = null;
	
	// Helper variables
	private K1 mKey;
	private V1 mValue;
	private K2 uKey;
	private V2 uValue;
	private K3 vKey;
	private V3 vValue;

	// Cache for all values from the V-Matrix
	private MatrixInputValueWritable[] vValues = new MatrixInputValueWritable[UVDecomposer.D_DIMENSION*UVDecomposer.V_INPUT_BLOCK_SIZE];

	private int lastRow = 0;
	private int matrixType = 1;
	private Float[] uArray = new Float[UVDecomposer.D_DIMENSION];
	
	public MatrixInputRecordReader(CompositeInputSplit split, JobConf conf, Reporter reporter) throws IOException {
		TupleValueInputFormat mFIF = new TupleValueInputFormat();
		mRecordReader = (RecordReader<K1, V1>) mFIF.getRecordReader(split.get(0), conf, reporter);
		
		UVTupleInputFormat uFIF = new UVTupleInputFormat();
		uRecordReader = (RecordReader<K2, V2>) uFIF.getRecordReader(split.get(1), conf, reporter);
		
		// Create uRecordReader
		UVTupleInputFormat vFIF = new UVTupleInputFormat();
		vRecordReader = (RecordReader<K3, V3>) vFIF.getRecordReader(split.get(2), conf, reporter);
		
		// Which matrix is worked on?
		matrixType = conf.getInt(UVDecomposer.MATRIX_TYPE, 1);
		
		// Create key/value pairs for parsing
		mKey = (K1) mRecordReader.createKey();
		mValue = (V1) mRecordReader.createValue();
		uKey = (K2) uRecordReader.createKey();
		uValue = (V2) uRecordReader.createValue();
		vKey = (K3) vRecordReader.createKey();
		vValue = (V3) vRecordReader.createValue();
		
		int i = 0;
		// Read all V-elements into Memory (smaller matrix)
		while(true) {
			if(vRecordReader.next(vKey, vValue)) {
				vValues[i] = new MatrixInputValueWritable(((MatrixInputValueWritable) vValue).getType(), ((MatrixInputValueWritable) vValue).getRow(), ((MatrixInputValueWritable) vValue).getColumn(), ((MatrixInputValueWritable) vValue).getValue());
				i++;
			} else {
				return;
			}
		}
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
	public boolean next(IntWritable key, InputWritable value) throws IOException {
		// As long as we have values from the M-Matrix, we look for the corresponding elements in U and V
		if(mRecordReader.next(mKey, mValue)) {
			int row = ((TupleValueWritable) mValue).getRow();
			int column = ((TupleValueWritable) mValue).getColumn();
			float grade = ((TupleValueWritable) mValue).getGrade();
			Float[] vArray = new Float[UVDecomposer.D_DIMENSION];
			
			// Are the elements from U already read?
			if(lastRow != row) {
				do {
					if(!uRecordReader.next(uKey, uValue)) {
						throw new IOException("Not corresponding U set");
					}
				} while(row != ((MatrixInputValueWritable) uValue).getRow());
				
				uArray[0] = ((MatrixInputValueWritable) uValue).getValue();
				for(int i = 1; i < UVDecomposer.D_DIMENSION; i++) {
					if(!uRecordReader.next(uKey, uValue)) 
						throw new IOException("Not corresponding U set");
					uArray[i] = ((MatrixInputValueWritable) uValue).getValue();
				}
			}
			lastRow = row;
			
			// Get corresponding 10 elements from V
			for(int i = ((column-1) % UVDecomposer.V_INPUT_BLOCK_SIZE) * UVDecomposer.D_DIMENSION, j = 0; j < UVDecomposer.D_DIMENSION; i++, j++) {
				vArray[j] = vValues[i].getValue();
			}
			
			// generate input record for mapper
			if(matrixType == UVDecomposer.MATRIX_U) {
				key.set(row);
				value.setValues(column, grade, uArray, vArray);
			} else {
				key.set(column);
				value.setValues(row, grade, uArray, vArray);
			}
			
			return true;
		}
		
		return false;
	}

	
	public IntWritable createKey() {
		return new IntWritable();
	}

	public InputWritable createValue() {
		return new InputWritable();
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
