package com.kruger.hadoop.matrixmatrix.twosteps;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class FirstMatrixReducer extends Reducer<Text, Text, Text, Text> {

	private final static Logger log = Logger.getLogger(FirstMatrixReducer.class);

	@Override
	protected void reduce(Text keyin, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Map<MatrixSide, List<IntermediateElement>> sides = StreamSupport.stream(values.spliterator(), false)
				.map(Text::toString).map(IntermediateElement::new)
				.collect(Collectors.groupingBy(IntermediateElement::getSide));

		List<IntermediateElement> leftElements = sides.getOrDefault(MatrixSide.LEFT, Collections.emptyList());
		List<IntermediateElement> rightElements = sides.getOrDefault(MatrixSide.RIGHT, Collections.emptyList());

		for (IntermediateElement l : leftElements) {
			for (IntermediateElement r : rightElements) {
				Text keyout = new Text(l.getNumber().toString() + "," + r.getNumber().toString());
				Text valueout = new Text(l.getValue().multiply(r.getValue()).toString());
				log.info("Emmiting key: " + keyout + " with value: " + valueout);
				context.write(keyout, valueout);
			}
		}
	}

}
