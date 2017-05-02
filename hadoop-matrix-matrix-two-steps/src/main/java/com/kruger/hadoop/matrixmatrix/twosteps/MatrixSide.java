package com.kruger.hadoop.matrixmatrix.twosteps;

import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum MatrixSide {

	LEFT {
		@Override
		String getTag() {
			return "L";
		}
	},
	RIGHT {
		@Override
		String getTag() {
			return "R";
		}
	};

	abstract String getTag();

	private static final Map<String, MatrixSide> tagToSide = Stream.of(MatrixSide.values())
			.collect(Collectors.toMap(MatrixSide::getTag, UnaryOperator.identity()));

	static MatrixSide getByTag(String tag) {
		MatrixSide side = tagToSide.get(tag);
		if (side == null) {
			throw new IllegalArgumentException(tag);
		}
		return side;
	}
}
