package com.kruger.hadoop.matrixmatrix.twosteps;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class FirstMatrixMapper extends Mapper<Text, Text, Text, Text> {

	protected static final Logger log = Logger.getLogger(FirstMatrixMapper.class);

	@Override
	protected void map(Text keyin, Text valuein, Context context) throws IOException, InterruptedException {
		MatrixElementCoord coord = new MatrixElementCoord(keyin.toString());

		Text keyout;
		Text valueout;

		if (coord.getSide() == MatrixSide.LEFT) {
			keyout = new Text(coord.getColumn().toString());
			valueout = new Text(coord.getSide().getTag() + "," + coord.getRow() + "," + valuein);
		} else {
			keyout = new Text(coord.getRow().toString());
			valueout = new Text(coord.getSide().getTag() + "," + coord.getColumn() + "," + valuein);
		}

		log.info("Mapping (" + keyin + ", " + valuein + ") -> (" + keyout + ", " + valueout + ")");

		context.write(keyout, valueout);
	}

}
