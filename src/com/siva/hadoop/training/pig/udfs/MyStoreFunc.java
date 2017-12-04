package com.siva.hadoop.training.pig.udfs;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pig.PigException;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class MyStoreFunc extends StoreFunc {
	protected RecordWriter writer = null;
	private byte fieldDel = '\t';
	private static final int BUFFER_SIZE = 1024;
	private static final String UTF8 = "UTF-8";
	private int num = 1;
	private int txt = 1;

	public MyStoreFunc() {
		this("\t");
	}

	public MyStoreFunc(String delimiter) {
		if (delimiter.length() == 1) {
			this.fieldDel = (byte) delimiter.charAt(0);
		} else if (delimiter.length() > 1 && delimiter.charAt(0) == '\\') {
			switch (delimiter.charAt(1)) {
			case 't':
				this.fieldDel = (byte) '\t';
				break;
			case 'x':
				fieldDel = Integer.valueOf(delimiter.substring(2), 16).byteValue();
				break;
			case 'u':
				this.fieldDel = Integer.valueOf(delimiter.substring(2)).byteValue();
				break;
			default:
				throw new RuntimeException("Unknown delimiter " + delimiter);
			}
		} else {
			throw new RuntimeException("PigStorage delimeter must be a single character");
		}
	}

	public MyStoreFunc(String delimiter, String properties) {
		this(delimiter);
		JSONParser parser = new JSONParser();
		try {
			JSONObject jsonObject = (JSONObject) parser.parse(properties);
			num = Integer.parseInt(jsonObject.get("number.multiply").toString());
			txt = Integer.parseInt(jsonObject.get("text.multiply").toString());
		} catch (ParseException e) {
			throw new RuntimeException("Storage properties not specified properly");
		}
	}

	ByteArrayOutputStream mOut = new ByteArrayOutputStream(BUFFER_SIZE);

	@Override
	public void putNext(Tuple f) throws IOException {
		int sz = f.size();
		for (int i = 0; i < sz; i++) {
			Object field;
			try {
				field = f.get(i);
			} catch (ExecException ee) {
				throw ee;
			}
			putField(field);
			if (i != sz - 1) {
				mOut.write(fieldDel);
			}
		}
		Text text = new Text(mOut.toByteArray());
		try {
			writer.write(null, text);
			mOut.reset();
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	@SuppressWarnings("unchecked")
	private void putField(Object field) throws IOException {
		// string constants for each delimiter
		String tupleBeginDelim = "(";
		String tupleEndDelim = ")";
		String bagBeginDelim = "{";
		String bagEndDelim = "}";
		String mapBeginDelim = "[";
		String mapEndDelim = "]";
		String fieldDelim = ",";
		String mapKeyValueDelim = "#";

		switch (DataType.findType(field)) {
		case DataType.NULL:
			break; // just leave it empty
		case DataType.BOOLEAN:
			mOut.write(((Boolean) field).toString().getBytes());
			break;
		case DataType.INTEGER:
			Integer i = (Integer) field * num;
			mOut.write(i.toString().getBytes());
			break;
		case DataType.LONG:
			Long l = (Long) field * num;
			mOut.write(l.toString().getBytes());
			break;
		case DataType.FLOAT:
			Float f = (Float) field * num;
			mOut.write(f.toString().getBytes());
			break;
		case DataType.DOUBLE:
			Double d = (Double) field * num;
			mOut.write(d.toString().getBytes());
			break;
		case DataType.BYTEARRAY: {
			byte[] b = ((DataByteArray) field).get();
			for (int k = 1; k <= txt; k++) {
				mOut.write(b, 0, b.length);
				if (k != txt)
					mOut.write(":".getBytes());
			}
			break;
		}
		case DataType.CHARARRAY:
			// oddly enough, writeBytes writes a string
			for (int k = 1; k <= txt; k++) {
				mOut.write(((String) field).getBytes(UTF8));
				if (k != txt)
					mOut.write(":".getBytes());
			}
			break;
		case DataType.MAP:
			boolean mapHasNext = false;
			Map<String, Object> m = (Map<String, Object>) field;
			mOut.write(mapBeginDelim.getBytes(UTF8));
			for (Map.Entry<String, Object> e : m.entrySet()) {
				if (mapHasNext) {
					mOut.write(fieldDelim.getBytes(UTF8));
				} else {
					mapHasNext = true;
				}
				putField(e.getKey());
				mOut.write(mapKeyValueDelim.getBytes(UTF8));
				putField(e.getValue());
			}
			mOut.write(mapEndDelim.getBytes(UTF8));
			break;
		case DataType.TUPLE:
			boolean tupleHasNext = false;
			Tuple t = (Tuple) field;
			mOut.write(tupleBeginDelim.getBytes(UTF8));
			for (int c = 0; c < t.size(); ++c) {
				if (tupleHasNext) {
					mOut.write(fieldDelim.getBytes(UTF8));
				} else {
					tupleHasNext = true;
				}
				try {
					putField(t.get(c));
				} catch (ExecException ee) {
					throw ee;
				}
			}
			mOut.write(tupleEndDelim.getBytes(UTF8));
			break;
		case DataType.BAG:
			boolean bagHasNext = false;
			mOut.write(bagBeginDelim.getBytes(UTF8));
			Iterator<Tuple> tupleIter = ((DataBag) field).iterator();
			while (tupleIter.hasNext()) {
				if (bagHasNext) {
					mOut.write(fieldDelim.getBytes(UTF8));
				} else {
					bagHasNext = true;
				}
				putField((Object) tupleIter.next());
			}
			mOut.write(bagEndDelim.getBytes(UTF8));
			break;
		default: {
			int errCode = 2108;
			String msg = "Could not determine data type of field: " + field;
			throw new ExecException(msg, errCode, PigException.BUG);
		}
		}
	}

	@Override
	public OutputFormat getOutputFormat() {
		return new TextOutputFormat<WritableComparable, Text>();
	}

	@Override
	public void prepareToWrite(RecordWriter writer) {
		this.writer = writer;
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		job.getConfiguration().set("mapred.textoutputformat.separator", "");
		FileOutputFormat.setOutputPath(job, new Path(location));
		if (location.endsWith(".bz2")) {
			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
		} else if (location.endsWith(".gz")) {
			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		}
	}
}