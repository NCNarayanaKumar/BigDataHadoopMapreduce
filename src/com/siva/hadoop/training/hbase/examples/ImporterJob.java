package com.siva.hadoop.training.hbase.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ImporterJob implements Tool {
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

		// create a new table with column family
		String tableName = "wordcount_input";
		String columnFamily = "cf";
		createTable(config, tableName, columnFamily);

		// initializing the job configuration
		Job importerJob = new Job(config);

		// setting the job name
		importerJob.setJobName("Orien IT HBase Importer Job");

		// to call this as a jar
		importerJob.setJarByClass(this.getClass());

		// setting custom mapper details
		importerJob.setMapperClass(ImporterMapper.class);

		// setting custom reducer details
		importerJob.setNumReduceTasks(0);

		// setting mapper output key class: K2
		importerJob.setOutputKeyClass(ImmutableBytesWritable.class);

		// setting mapper output value class: V2
		importerJob.setOutputValueClass(Put.class);

		// setting the input format class ,i.e for K1, V1
		importerJob.setInputFormatClass(TextInputFormat.class);

		// setting the output format class
		importerJob.setOutputFormatClass(TableOutputFormat.class);
		importerJob.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);

		// setting the input file path
		FileInputFormat.addInputPath(importerJob, new Path(args[0]));

		// to execute the job and return the status
		return importerJob.waitForCompletion(true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
		// start the job providing arguments and configurations
		if (args.length < 1) {
			System.err.print("Provide the input file");
			System.exit(0);
		}
		int status = ToolRunner.run(new Configuration(), new ImporterJob(), args);
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

class ImporterMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	public static final byte[] CF = "cf".getBytes();
	public static final byte[] ATTR = "line".getBytes();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.add(CF, ATTR, Bytes.toBytes(value.toString()));
		context.write(null, put);
	}
}