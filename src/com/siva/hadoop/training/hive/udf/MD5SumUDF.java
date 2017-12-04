package com.siva.hadoop.training.hive.udf;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

@Description(name = "md5sum", value = "_FUNC_(string) - computes the MD5 sum of the input string.  Returns the hash in hexidecimal format.", extended = "Example: _FUNC_('foobar') = 3858f62230ac3c915f300c664312c63f")
public class MD5SumUDF extends UDF {

	public Text evaluate(Text input) {
		String md5Hex = DigestUtils.md5Hex(input.toString());
		return new Text(md5Hex);
	}

	public Text evaluate(Text input1, Text input2) {
		String concat = input1.toString().concat(input2.toString());
		String md5Hex = DigestUtils.md5Hex(concat);
		return new Text(md5Hex);
	}
}
