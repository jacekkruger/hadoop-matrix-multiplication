package com.kruger.mapreduce.matrixsmallvector;

import java.math.BigInteger;

public class MatrixElementCoord {
	private final BigInteger row;
	private final BigInteger column;

	public MatrixElementCoord(String line) {
		String split[] = line.split(",", 2);
		row = new BigInteger(split[0]);
		column = new BigInteger(split[1]);
	}

	public BigInteger getRow() {
		return row;
	}

	public BigInteger getColumn() {
		return column;
	}
}
