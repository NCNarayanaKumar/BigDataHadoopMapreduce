package com.siva.hadoop.training.temperature;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TempReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	@Override
	protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
		float temp = getMaxValue(values);
		// float temp = getMinValue(values);
		context.write(key, new FloatWritable(temp));
	}

	private float getMaxValue(Iterable<FloatWritable> values) {
		float temp = Float.MIN_VALUE;
		for (FloatWritable value : values) {
			float ctemp = value.get();
			if (ctemp >= temp) {
				temp = ctemp;
			}
		}
		return temp;
	}

	private float getMinValue(Iterable<FloatWritable> values) {
		float temp = Float.MAX_VALUE;
		for (FloatWritable value : values) {
			float ctemp = value.get();
			if (ctemp <= temp) {
				temp = ctemp;
			}
		}
		return temp;
	}

}
