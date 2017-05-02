package com.kruger.hadoop.matrixmatrix.twosteps;

import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MatrixMatrix {

	public static final String LEFT_MATRIX_PARAM = "matrix-matrix-twosteps.left";
	public static final String RIGHT_MATRIX_PARAM = "matrix-matrix-twosteps.right";
	public static final String TEMP_PARAM = "matrix-matrix-twosteps.temp";
	public static final String OUTPUT_PARAM = "matrix-matrix-twosteps.output";

	public static final String PARAMS[] = { LEFT_MATRIX_PARAM, RIGHT_MATRIX_PARAM, TEMP_PARAM, OUTPUT_PARAM };

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		new GenericOptionsParser(conf, args);

		for (String param : PARAMS) {
			Objects.requireNonNull(conf.get(param), "Parameter " + param + " is required");
		}

		Job firstJob = Job.getInstance(conf, "Matrix × Matrix - first part");
		firstJob.setJarByClass(MatrixMatrix.class);
		firstJob.setMapperClass(FirstMatrixMapper.class);
		// job.setCombinerClass(MatrixReducer.class);
		firstJob.setReducerClass(FirstMatrixReducer.class);
		firstJob.setOutputKeyClass(Text.class);
		firstJob.setInputFormatClass(KeyValueTextInputFormat.class);
		firstJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(firstJob, new Path(conf.get(MatrixMatrix.LEFT_MATRIX_PARAM)));
		FileInputFormat.addInputPath(firstJob, new Path(conf.get(MatrixMatrix.RIGHT_MATRIX_PARAM)));
		FileOutputFormat.setOutputPath(firstJob, new Path(conf.get(MatrixMatrix.TEMP_PARAM)));

		if (!firstJob.waitForCompletion(true)) {
			System.err.println("First job failed");
			System.exit(1);
		}

		Job secondJob = Job.getInstance(conf, "Matrix × Matrix - second part");
		secondJob.setJarByClass(MatrixMatrix.class);
		secondJob.setMapperClass(SecondMatrixMapper.class);
		// job.setCombinerClass(MatrixReducer.class);
		secondJob.setReducerClass(SecondMatrixReducer.class);
		secondJob.setOutputKeyClass(Text.class);
		secondJob.setInputFormatClass(KeyValueTextInputFormat.class);
		secondJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(secondJob, new Path(conf.get(MatrixMatrix.TEMP_PARAM)));
		FileOutputFormat.setOutputPath(secondJob, new Path(conf.get(MatrixMatrix.OUTPUT_PARAM)));

		if (!secondJob.waitForCompletion(true)) {
			System.err.println("Second job failed");
			System.exit(1);
		}

	}
}
