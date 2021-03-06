package com.kruger.mapreduce.matrixvector;

import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MatrixVector {

	public static final String MATRIX_PARAM = "matrix-vector.matrix";
	public static final String VECTOR_PARAM = "matrix-vector.vector";
	public static final String OUTPUT_PARAM = "matrix-vector.output";

	public static final String PARAMS[] = { MATRIX_PARAM, VECTOR_PARAM, OUTPUT_PARAM };

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		new GenericOptionsParser(conf, args);

		for (String param : PARAMS) {
			Objects.requireNonNull(conf.get(param), "Parameter " + param + " is required");
		}

		Job job = Job.getInstance(conf, "Matrix × Vector");
		job.setJarByClass(MatrixVector.class);
		job.setMapperClass(MatrixMapper.class);
		// job.setCombinerClass(MatrixReducer.class);
		job.setReducerClass(MatrixReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(conf.get(MatrixVector.MATRIX_PARAM)));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(MatrixVector.OUTPUT_PARAM)));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
