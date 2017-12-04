package com.siva.hadoop.training.pig.udfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.FilterFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class IsGoodQuality extends FilterFunc {

	@Override
	public Boolean exec(Tuple tuple) throws IOException {
		if (tuple == null || tuple.size() == 0) {
			return false;
		}
		try {
			Object object = tuple.get(0);
			if (object == null) {
				return false;
			}
			int quality = (Integer) object;
			return quality == 0 || quality == 1 || quality == 4 || quality == 5 || quality == 9;
		} catch (ExecException e) {
			throw new IOException(e);
		}
	}

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FuncSpec> funcSpecs = new ArrayList<FuncSpec>();
		funcSpecs.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.INTEGER))));
		return funcSpecs;
	}

}


















