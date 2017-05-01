package com.kruger.mapreduce.matrixvector;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class MatrixMapper extends Mapper<Text, Text, Text, Text> {

	protected static final Logger log = Logger.getLogger(MatrixMapper.class);

	protected LazyVector vector;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		FileSystem fs = FileSystem.get(configuration);
		Path vectorPath = new Path(configuration.get(MatrixVector.VECTOR_PARAM));
		vector = new LazyVector(fs.open(vectorPath));
	}

	@Override
	protected void cleanup(Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		vector.cleanup();
	}

	@Override
	protected void map(Text keyin, Text valuein, Context context) throws IOException, InterruptedException {
		MatrixElementCoord coord = new MatrixElementCoord(keyin.toString());
		BigInteger value = new BigInteger(valuein.toString());

		BigInteger result = value.multiply(vector.get(coord.getColumn()));

		Text keyout = new Text(coord.getRow().toString());
		Text valueout = new Text(result.toString());

		log.info("Mapping (" + keyin + ", " + valuein + ") -> (" + keyout + ", " + valueout + ")");

		context.write(keyout, valueout);
	}

}
