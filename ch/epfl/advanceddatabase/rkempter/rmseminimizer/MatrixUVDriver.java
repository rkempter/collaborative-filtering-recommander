package ch.epfl.advanceddatabase.rkempter.rmseminimizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.MultipleInputs;

import ch.epfl.advanceddatabase.rkempter.UVDecomposer;
import ch.epfl.advanceddatabase.rkempter.initialization.TupleValueInputFormat;

public class MatrixUVDriver {
	
	public static JobConf getNewUVConf(JobConf conf, int matrix, int xPos) {
		conf.setJobName("UV Conf");
		conf.setInputFormat(MatrixUVInputFormat.class);
		conf.setOutputFormat(MatrixUVOutputFormat.class);
		
		conf.setInt(UVDecomposer.MATRIX_X_POSITION, xPos);
		conf.setInt(UVDecomposer.MATRIX_TYPE, matrix);
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(MatrixInputValueWritable.class);
		conf.setCombinerClass(MatrixCombiner.class);
		
		FileOutputFormat.setOutputPath(conf, new Path(UVDecomposer.I_PATH));
		
		conf.setMapperClass(MatrixUVMapper.class);
		conf.setReducerClass(MatrixUVReducer.class);
		
		
		
		// Multi Input Path
		//MultipleInputs.addInputPath(conf, new Path("/std44/output/"), MatrixUVInputFormat.class, MatrixUVMapper.class);
		//MultipleInputs.addInputPath(conf, new Path(UVDecomposer.M_PATH), TupleValueInputFormat.class, MatrixMMapper.class);
		MatrixUVInputFormat.setUInputInfo(conf, UVTupleInputFormat.class, UVDecomposer.U_PATH);
		MatrixUVInputFormat.setVInputInfo(conf, UVTupleInputFormat.class, UVDecomposer.V_PATH);
		
		return conf;
	}

	// Not done yet.
	public JobConf getNewMConf(String input, int xPos) {
		JobConf conf = new JobConf();
		
		conf.setJobName("M Conf");
		conf.setOutputFormat(MatrixUVOutputFormat.class);
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(MatrixInputValueWritable.class);
		
		return conf;
		
	}

}
