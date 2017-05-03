package com.kruger.hadoop.matrixmatrix;

import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

public class MatrixMatrix {

	protected static final Logger log = Logger.getLogger(MatrixMatrix.class);

	public static final String LEFT_MATRIX_PARAM = "matrix-matrix.left";
	public static final String RIGHT_MATRIX_PARAM = "matrix-matrix.right";
	public static final String OUTPUT_PARAM = "matrix-matrix.output";
	public static final String OUTPUT_MATRIX_SIZE_PARAM = "matrix-matrix.output.size";

	public static final String PARAMS[] = { LEFT_MATRIX_PARAM, RIGHT_MATRIX_PARAM, OUTPUT_PARAM,
			OUTPUT_MATRIX_SIZE_PARAM };

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		new GenericOptionsParser(conf, args);

		for (String param : PARAMS) {
			Objects.requireNonNull(conf.get(param), "Parameter " + param + " is required");
		}

		Job job = Job.getInstance(conf, "Matrix × Matrix");
		job.setJarByClass(MatrixMatrix.class);
		job.setMapperClass(MatrixMapper.class);
		// job.setCombinerClass(MatrixReducer.class);
		job.setReducerClass(MatrixReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(conf.get(MatrixMatrix.LEFT_MATRIX_PARAM)));
		FileInputFormat.addInputPath(job, new Path(conf.get(MatrixMatrix.RIGHT_MATRIX_PARAM)));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(MatrixMatrix.OUTPUT_PARAM)));

		if (!job.waitForCompletion(true)) {
			log.error("Job failed");
			System.exit(1);
		}
	}
}
