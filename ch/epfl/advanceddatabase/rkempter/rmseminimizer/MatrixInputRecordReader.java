package ch.epfl.advanceddatabase.rkempter.rmseminimizer;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.join.CompositeInputSplit;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

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
	private BloomFilter filter = new BloomFilter();
	private int rowCounter = 0;
	private int columnCounter = 0;
	
	private MatrixInputValueWritable[] uValues = new MatrixInputValueWritable[UVDecomposer.D_DIMENSION];
	private MatrixInputValueWritable[] vValues = new MatrixInputValueWritable[UVDecomposer.D_DIMENSION*UVDecomposer.BLOCK_SIZE];
	private boolean uRead = false, vRead = false;
	private int uIndex = 0, vIndex = 0;
	private int setMaxColumn = UVDecomposer.BLOCK_SIZE * UVDecomposer.D_DIMENSION - 1;
	
	public MatrixInputRecordReader(CompositeInputSplit split, JobConf conf, Reporter reporter) throws IOException {
		
		// Create vRecordReader
		UVTupleInputFormat uFIF = null;
		this.vConf = conf;
		this.vSplit = split.get(0);
		this.vReporter = reporter;
		
		Path bfPath = new Path("/std44/output/B/part-00000");
		FileSystem fs = bfPath.getFileSystem(conf);
		DataInputStream stream = fs.open(bfPath);
		this.filter.readFields(stream);
		stream.close();
		
		System.out.println("Filter vector size: "+filter.getVectorSize());
		System.out.println("Filterstring: "+filter.toString());
		System.out.println("IN Record reader. Filter size: "+filter.getVectorSize());
		
		
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
		boolean sent = false;
		
		while(!sent) {
			if(!vRead) {
				if(vRecordReader.next(vKey, vValue)) {
					MatrixInputValueWritable vVal = (MatrixInputValueWritable) vValue;
					columnCounter = vVal.getColumn();
					vValues[vIndex] = new MatrixInputValueWritable(vVal.getType(), vVal.getRow(), vVal.getColumn(), vVal.getValue());
				} else {
					setMaxColumn = vIndex;
					vIndex = 0;
					uIndex = 0;
					uRead = false;
					vRead = true;
				}
			}
			
			if(!uRead) {
				if(uRecordReader.next(uKey, uValue)) {
					MatrixInputValueWritable uVal = (MatrixInputValueWritable) uValue;
					rowCounter = uVal.getRow();
					uValues[uIndex] = new MatrixInputValueWritable(uVal.getType(), uVal.getRow(), uVal.getColumn(), uVal.getValue());
				} else {
					return false;
				}
			}
			
			int indexToTest = (rowCounter-1) * UVDecomposer.NBR_MOVIES + columnCounter;
			byte[] indexToTestBytes = Integer.toString(indexToTest).getBytes();
			Key indexKey = new Key(indexToTestBytes);
			if(this.filter.membershipTest(indexKey)) {
				System.out.println("Filter passed!!!!");
				value.setValues(vValues[vIndex].getType(), vValues[vIndex].getRow(), vValues[vIndex].getColumn(), vValues[vIndex].getValue());
				key.setValues(uValues[uIndex].getType(), uValues[uIndex].getRow(), uValues[uIndex].getColumn(), uValues[uIndex].getValue());
				sent = true;
			}	
			uIndex++;
			vIndex++;
			
			
			if(uIndex == UVDecomposer.D_DIMENSION) {
				uIndex = 0;
				uRead = true;
			}
			
			if(vIndex == setMaxColumn) {
				vIndex = 0;
				uIndex = 0;
				uRead = false;
				vRead = true;
			}
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
