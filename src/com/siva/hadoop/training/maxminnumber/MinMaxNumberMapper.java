package com.siva.hadoop.training.maxminnumber;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinMaxNumberMapper extends Mapper<LongWritable, Text, Text, Text> {

	static Double min = Double.MIN_VALUE;
	static Double max = Double.MAX_VALUE;

	Map<String, Double> colMin;
	Map<String, Double> colMax;
	int noCol;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		colMin = new LinkedHashMap<String, Double>();
		colMax = new LinkedHashMap<String, Double>();

		Configuration conf = context.getConfiguration();
		noCol = Integer.parseInt(conf.get("no.of.columns"));

		for (int i = 0; i < noCol; i++) {
			String key = "col_" + i;
			colMin.put(key, max);
			colMax.put(key, min);
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] columns = line.split(",");

		for (int i = 0; i < columns.length; i++) {
			String keyi = "col_" + i;
			Double col;
			try {
				col = Double.valueOf(columns[i]);
				if (col != null) {
					Double valMin = colMin.get(keyi);
					Double valMax = colMax.get(keyi);
					double max2 = Math.max(valMax, col);
					double min2 = Math.min(valMin, col);
					colMin.put(keyi, new Double(min2));
					colMax.put(keyi, new Double(max2));
				}
			} catch (Exception e) {
			}

		}
	};

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		String minRow = getString(colMin);
		String maxRow = getString(colMax);

		context.write(new Text("min"), new Text(minRow));
		context.write(new Text("max"), new Text(maxRow));
	}

	private String getString(Map<String, Double> map) {
		Set<Entry<String, Double>> entrySet = map.entrySet();
		String mydata = "";
		for (Entry<String, Double> entry : entrySet) {
			String data = entry.getKey() + "::" + entry.getValue();
			mydata = mydata + "," + data;
		}
		return mydata.substring(1);
	}

}
