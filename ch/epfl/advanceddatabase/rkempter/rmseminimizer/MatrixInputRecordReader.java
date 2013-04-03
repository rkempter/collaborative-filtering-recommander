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
	
	private boolean allDone = false;
	
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
		
		int i = 0, j = 0;;
		boolean goToNextU = true, allDone = false;
		MatrixInputValueWritable[] uValues = new MatrixInputValueWritable[UVDecomposer.D_DIMENSION];
		MatrixInputValueWritable[] vValues = new MatrixInputValueWritable[UVDecomposer.D_DIMENSION*UVDecomposer.BLOCK_SIZE];
		
		while(vRecordReader.next(vKey, vValue)) {
			MatrixInputValueWritable vVal = (MatrixInputValueWritable) uValue;
			vValues[i] = new MatrixInputValueWritable(vVal.getType(), vVal.getRow(), vVal.getColumn(), vVal.getValue());
			i++;
		}
		
		if(goToNextU) {
			for(int k = 0; k < UVDecomposer.D_DIMENSION; k++) {
				if(uRecordReader.next(uKey, uValue)) {
					MatrixInputValueWritable uVal = (MatrixInputValueWritable) uValue;
					uValues[k] = new MatrixInputValueWritable(uVal.getType(), uVal.getRow(), uVal.getColumn(), uVal.getValue());
					goToNextU = false;
				} else {
					allDone = true;
				}
			}
			
			if(!allDone) {
				do {
					if(j < vValues.length) {
						int index = j % UVDecomposer.D_DIMENSION;
						String debug = String.format("Urow: %d,  Ucolumn: %d, Vrow: %d, Vcolumn: %d, part of %d/%d", uValues[index].getRow(), uValues[index].getColumn(), uValues[j].getRow(), uValues[j].getColumn(), uValues[index].getRow(),uValues[j].getColumn());
						System.out.println(debug);
						value.setValues(uValues[j].getType(), uValues[j].getRow(), uValues[j].getColumn(), uValues[j].getValue());
						key.setValues(uValues[index].getType(), uValues[index].getRow(), uValues[index].getColumn(), uValues[index].getValue());
						i++;
						if(i == UVDecomposer.D_DIMENSION) {
							i = 0;
						}
					} else {
						goToNextU = true;
					}
				} while(!goToNextU);
			}		
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
