package com.kruger.mapreduce.matrixsmallvector;

import java.io.IOException;
import java.math.BigInteger;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class MatrixReducer extends Reducer<Text, Text, Text, Text> {

	private final static Logger log = Logger.getLogger(MatrixReducer.class);

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		BigInteger sum = StreamSupport.stream(values.spliterator(), false).map(Text::toString).map(BigInteger::new)
				.reduce(BigInteger.ZERO, BigInteger::add);

		log.info("Emmiting key: " + key + " with value: " + sum);

		context.write(key, new Text(sum.toString()));
	}

}
