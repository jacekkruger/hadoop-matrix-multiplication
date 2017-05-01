package com.kruger.mapreduce.matrixvector;

import java.math.BigInteger;

public class VectorElement {
	private final BigInteger row;
	private final BigInteger value;

	public VectorElement(String line) {
		String split[] = line.split("\t", 2);
		row = new BigInteger(split[0]);
		value = new BigInteger(split[1]);
	}

	public BigInteger getRow() {
		return row;
	}

	public BigInteger getValue() {
		return value;
	}

}
