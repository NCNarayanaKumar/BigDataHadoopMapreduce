package com.siva.hadoop.training.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

@Description(name = "double_my_data", value = "_FUNC_(string,string) - double the input string with separator.  Returns the date in string format.", extended = "Example: _FUNC_('sample',':') = sample:sample")
public class DoubleMyDataUDF extends UDF {

	public Text evaluate(Text input, Text separator) {
		String result = StringUtils.repeat(input.toString(), separator.toString(), 2);
		return new Text(result);
	}
}
