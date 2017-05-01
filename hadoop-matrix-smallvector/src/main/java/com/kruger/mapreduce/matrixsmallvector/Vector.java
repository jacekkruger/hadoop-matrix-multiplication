package com.kruger.mapreduce.matrixsmallvector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

public class Vector {

	protected static final Logger log = Logger.getLogger(Vector.class);

	protected Map<BigInteger, BigInteger> vector;

	public Vector(InputStream vectorData) throws IOException {
		try (BufferedReader is = new BufferedReader(new InputStreamReader(vectorData))) {
			vector = is.lines().map(VectorElement::new)
					.collect(Collectors.toMap(VectorElement::getRow, VectorElement::getValue));
		}
		log.info("Loaded vector: " + vector);
	}

	public BigInteger get(BigInteger row) {
		return vector.getOrDefault(row, BigInteger.ZERO);
	}
}
