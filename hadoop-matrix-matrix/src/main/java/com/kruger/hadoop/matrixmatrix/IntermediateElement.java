package com.kruger.hadoop.matrixmatrix;

import java.math.BigInteger;

public class IntermediateElement {
	private final MatrixSide side;
	private final BigInteger number;
	private final BigInteger value;

	public IntermediateElement(String line) {
		String split[] = line.split(",", 3);
		side = MatrixSide.getByTag(split[0]);
		number = new BigInteger(split[1]);
		value = new BigInteger(split[2]);
	}

	public MatrixSide getSide() {
		return side;
	}

	public BigInteger getNumber() {
		return number;
	}

	public BigInteger getValue() {
		return value;
	}

}
