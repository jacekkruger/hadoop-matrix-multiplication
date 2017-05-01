package com.kruger.mapreduce.matrixvector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;

import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

public class LazyVector {

	protected static final Logger log = Logger.getLogger(LazyVector.class);

	protected BufferedReader inputStream;

	protected VectorElement lastElement;

	protected PeekingIterator<VectorElement> vectorElements;

	public LazyVector(InputStream vectorData) throws IOException {
		inputStream = new BufferedReader(new InputStreamReader(vectorData));
		vectorElements = Iterators.peekingIterator(inputStream.lines().map(VectorElement::new).iterator());
	}

	public void cleanup() throws IOException {
		inputStream.close();
	}

	public BigInteger get(BigInteger row) {
		while (vectorElements.hasNext()) {
			if (vectorElements.peek().getRow().compareTo(row) == 0) {
				log.info("Using element " + vectorElements.peek().getRow() + " -> " + vectorElements.peek().getValue());
				return vectorElements.peek().getValue();
			} else if (vectorElements.peek().getRow().compareTo(row) < 0) {
				log.info("Advancing vector element: " + vectorElements.peek().getRow() + " ... " + row);
				vectorElements.next();
			} else if (vectorElements.peek().getRow().compareTo(row) > 0) {
				log.error("Accessing vector element in decreasing order: " + vectorElements.peek().getRow() + " ... "
						+ row);
				throw new IllegalStateException("Trying to access vector elements in decreasing order");
			}
		}
		return BigInteger.ZERO;
	}
}
