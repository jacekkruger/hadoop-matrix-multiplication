package com.kruger.hadoop.matrixmatrix;

import java.math.BigInteger;

public class MatrixElementCoord {
	private final MatrixSide side;
	private final BigInteger row;
	private final BigInteger column;

	public MatrixElementCoord(String line) {
		String split[] = line.split(",", 3);
		side = MatrixSide.getByTag(split[0]);
		row = new BigInteger(split[1]);
		column = new BigInteger(split[2]);
	}

	public MatrixSide getSide() {
		return side;
	}

	public BigInteger getRow() {
		return row;
	}

	public BigInteger getColumn() {
		return column;
	}

}
