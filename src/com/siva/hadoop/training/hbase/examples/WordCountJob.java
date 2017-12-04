package com.siva.hadoop.training.hbase.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountJob implements Tool {
	// Initializing configuration object
	private Configuration conf;

	@Override
	public Configuration getConf() {
		return conf; // getting the configuration
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf; // setting the configuration
	}

	@Override
	public int run(String[] args) throws Exception {
		// initializing the hbase configuration object
		Configuration config = HBaseConfiguration.create(getConf());

		// initializing the job configuration
		Job wordCountJob = new Job(config);

		// setting the job name
		wordCountJob.setJobName("Orien IT HBase WordCount Job");

		// to call this as a jar
		wordCountJob.setJarByClass(this.getClass());

		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);

		String inputTable = "wordcount_input";
		String outputTable = "wordcount_output";

		// create a new table with column family
		String columnFamily = "cf";
		createTable(config, outputTable, columnFamily);

		// setting custom mapper details
		TableMapReduceUtil.initTableMapperJob(inputTable, scan, WordCountMapper.class, Text.class, LongWritable.class, wordCountJob);

		// setting custom reducer details
		TableMapReduceUtil.initTableReducerJob(outputTable, WordCountReducer.class, wordCountJob);

		// setting the input format class ,i.e for K1, V1
		wordCountJob.setInputFormatClass(TableInputFormat.class);

		// setting the output format class
		wordCountJob.setOutputFormatClass(TableOutputFormat.class);
		wordCountJob.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, outputTable);

		// to execute the job and return the status
		return wordCountJob.waitForCompletion(true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
		// start the job providing arguments and configurations
		int status = ToolRunner.run(new Configuration(), new WordCountJob(), args);
		System.out.println("My Status: " + status);
	}

	private void createTable(Configuration conf, String name, String columnFamily) throws IOException {
		Connection connection = ConnectionFactory.createConnection(conf);
		try {
			Admin admin = connection.getAdmin();
			TableName tableName = TableName.valueOf(name);
			boolean tableExists = admin.tableExists(tableName);
			if (tableExists) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}

			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);
			tableDescriptor.addFamily(columnDescriptor);
			admin.createTable(tableDescriptor);
		} finally {
			connection.close();
		}
	}
}

class WordCountMapper extends TableMapper<Text, LongWritable> {
	public static final byte[] CF = "cf".getBytes();
	public static final byte[] ATTR = "line".getBytes();
	private static final LongWritable ONE = new LongWritable(1);

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		String line = new String(value.getValue(CF, ATTR));
		String[] words = line.split(" ");
		for (String word : words) {
			context.write(new Text(word), ONE);
		}
	}
}

class WordCountReducer extends TableReducer<Text, LongWritable, ImmutableBytesWritable> {
	public static final byte[] CF = "cf".getBytes();
	public static final byte[] COUNT = "count".getBytes();

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long sum = 0;
		for (LongWritable val : values) {
			sum += val.get();
		}
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.add(CF, COUNT, Bytes.toBytes(String.valueOf(sum)));
		context.write(null, put);
	}
}
