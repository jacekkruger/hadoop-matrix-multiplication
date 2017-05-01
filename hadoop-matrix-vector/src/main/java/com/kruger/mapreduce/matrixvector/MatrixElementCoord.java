package com.kruger.mapreduce.matrixvector;

import java.math.BigInteger;

public class MatrixElementCoord {
	private final BigInteger column;
	private final BigInteger row;

	public MatrixElementCoord(String line) {
		String split[] = line.split(",", 2);
		column = new BigInteger(split[0]);
		row = new BigInteger(split[1]);
	}

	public BigInteger getRow() {
		return row;
	}

	public BigInteger getColumn() {
		return column;
	}
}
