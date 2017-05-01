package com.kruger.mapreduce.matrixsmallvector;

import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MatrixSmallVector {

	public static final String MATRIX_PARAM = "matrix-smallvector.matrix";
	public static final String VECTOR_PARAM = "matrix-smallvector.vector";
	public static final String OUTPUT_PARAM = "matrix-smallvector.output";

	public static final String PARAMS[] = { MATRIX_PARAM, VECTOR_PARAM, OUTPUT_PARAM };

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		new GenericOptionsParser(conf, args);

		for (String param : PARAMS) {
			Objects.requireNonNull(conf.get(param), "Parameter " + param + " is required");
		}

		Job job = Job.getInstance(conf, "Matrix × SmallVector");
		job.setJarByClass(MatrixSmallVector.class);
		job.setMapperClass(MatrixMapper.class);
		// job.setCombinerClass(MatrixReducer.class);
		job.setReducerClass(MatrixReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(conf.get(MatrixSmallVector.MATRIX_PARAM)));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(MatrixSmallVector.OUTPUT_PARAM)));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
