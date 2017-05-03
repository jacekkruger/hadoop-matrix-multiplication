package com.kruger.hadoop.matrixmatrix;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class MatrixMapper extends Mapper<Text, Text, Text, Text> {

	protected static final Logger log = Logger.getLogger(MatrixMapper.class);

	protected OutputSize outputSize;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		outputSize = new OutputSize(context.getConfiguration().get(MatrixMatrix.OUTPUT_MATRIX_SIZE_PARAM));
	}

	@Override
	protected void map(Text keyin, Text valuein, Context context) throws IOException, InterruptedException {
		MatrixElementCoord coord = new MatrixElementCoord(keyin.toString());

		if (coord.getSide() == MatrixSide.LEFT) {
			for (BigInteger i = BigInteger.ONE; i.compareTo(outputSize.getColumns()) <= 0; i = i.add(BigInteger.ONE)) {
				write(context, keyin, valuein, coord.getRow() + "," + i,
						coord.getSide().getTag() + "," + coord.getColumn() + "," + valuein);
			}
		} else {
			for (BigInteger i = BigInteger.ONE; i.compareTo(outputSize.getRows()) <= 0; i = i.add(BigInteger.ONE)) {
				write(context, keyin, valuein, i + "," + coord.getColumn(),
						coord.getSide().getTag() + "," + coord.getRow() + "," + valuein);
			}
		}
	}

	protected void write(Context context, Text keyin, Text valuein, String keyout, String valueout)
			throws IOException, InterruptedException {
		log.info("Mapping (" + keyin + ", " + valuein + ") -> (" + keyout + ", " + valueout + ")");

		context.write(new Text(keyout), new Text(valueout));
	}

}
