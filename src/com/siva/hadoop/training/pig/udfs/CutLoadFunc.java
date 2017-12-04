package com.siva.hadoop.training.pig.udfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class CutLoadFunc extends LoadFunc {

	private static final Log LOG = LogFactory.getLog(CutLoadFunc.class);

	private final List<Range> ranges;
	private final TupleFactory tupleFactory = TupleFactory.getInstance();
	private RecordReader reader;

	public CutLoadFunc(String cutPattern) {
		ranges = Range.parse(cutPattern);
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, location);
	}

	@Override
	public InputFormat getInputFormat() {
		return new TextInputFormat();
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split) {
		this.reader = reader;
	}

	@Override
	public Tuple getNext() throws IOException {
		try {
			if (!reader.nextKeyValue()) {
				return null;
			}
			Text value = (Text) reader.getCurrentValue();
			String line = value.toString();
			Tuple tuple = tupleFactory.newTuple(ranges.size());
			for (int i = 0; i < ranges.size(); i++) {
				Range range = ranges.get(i);
				if (range.getEnd() > line.length()) {
					LOG.warn(String.format("Range end (%s) is longer than line length (%s)", range.getEnd(), line.length()));
					continue;
				}
				tuple.set(i, new DataByteArray(range.getSubstring(line)));
			}
			return tuple;
		} catch (InterruptedException e) {
			throw new ExecException(e);
		}
	}
}

class Range {
	private final int start;
	private final int end;

	public Range(int start, int end) {
		this.start = start;
		this.end = end;
	}

	public int getStart() {
		return start;
	}

	public int getEnd() {
		return end;
	}

	public String getSubstring(String line) {
		return line.substring(start - 1, end);
	}

	@Override
	public int hashCode() {
		return start * 37 + end;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Range)) {
			return false;
		}
		Range other = (Range) obj;
		return this.start == other.start && this.end == other.end;
	}

	public static List<Range> parse(String rangeSpec) throws IllegalArgumentException {
		if (rangeSpec.length() == 0) {
			return Collections.emptyList();
		}
		List<Range> ranges = new ArrayList<Range>();
		String[] specs = rangeSpec.split(",");
		for (String spec : specs) {
			String[] split = spec.split("-");
			try {
				int start = Integer.parseInt(split[0]);
				int end = Integer.parseInt(split[1]);
				ranges.add(new Range(start, end));
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException(e.getMessage());
			}
		}
		return ranges;
	}

}
