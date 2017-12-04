package com.siva.hadoop.training.hive.SerDe;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

@SerDeSpec(schemaProps = { serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES, ColumnMultiplySerDe.TEXT_MULTIPLY,
		ColumnMultiplySerDe.NUMBER_MULTIPLY })
public class ColumnMultiplySerDe extends AbstractSerDe {

	public static final Logger LOG = LoggerFactory.getLogger(ColumnMultiplySerDe.class.getName());

	private List<TypeInfo> columnTypes;
	private List<String> columnNames;
	private List<Object> row;
	private Map<String, Object> rowMap;
	private int numColumns;
	private StructObjectInspector objectInspector;

	public static final String NUMBER_MULTIPLY = "number.multiply";
	public static final String TEXT_MULTIPLY = "text.multiply";

	int num_multiply;
	int txt_multiply;

	@Override
	public void initialize(Configuration conf, Properties tbl) throws SerDeException {
		// Read the configuration parameters
		String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
		String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
		String number_multiply = tbl.getProperty(NUMBER_MULTIPLY);
		String text_multiply = tbl.getProperty(TEXT_MULTIPLY);

		// Parse the configuration parameters
		if (number_multiply != null) {
			num_multiply = Integer.parseInt(number_multiply);
		} else {
			throw new SerDeException("This table does not have serde property \"number.multiply\"!");
		}

		// Parse the configuration parameters
		if (text_multiply != null) {
			txt_multiply = Integer.parseInt(text_multiply);
		} else {
			throw new SerDeException("This table does not have serde property \"text.multiply\"!");
		}

		columnNames = Arrays.asList(columnNameProperty.split(","));
		columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
		numColumns = columnNames.size();

		/*
		 * Constructing the row ObjectInspector: The row consists of some set of
		 * primitive columns, each column will be a java object of primitive
		 * type.
		 */
		List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(columnNames.size());
		for (int c = 0; c < numColumns; c++) {
			TypeInfo typeInfo = columnTypes.get(c);
			if (typeInfo instanceof PrimitiveTypeInfo) {
				PrimitiveTypeInfo pti = (PrimitiveTypeInfo) columnTypes.get(c);
				AbstractPrimitiveJavaObjectInspector oi = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(pti);
				columnOIs.add(oi);
			} else {
				throw new SerDeException(getClass().getName() + " doesn't allow column [" + c + "] named " + columnNames.get(c)
						+ " with type " + columnTypes.get(c));
			}
		}

		// StandardStruct uses ArrayList to store the row.
		objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs,
				Lists.newArrayList(Splitter.on('\0').split(tbl.getProperty("columns.comments"))));

		row = new ArrayList<Object>(numColumns);
		// Constructing the row object, etc, which will be reused for all rows.
		for (int c = 0; c < numColumns; c++) {
			row.add(null);
		}

		rowMap = new HashMap<String, Object>(columnNames.size());
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return objectInspector;
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return Text.class;
	}

	@Override
	public Object deserialize(Writable blob) throws SerDeException {
		Text text = (Text) blob;
		String content = text.toString();
		String[] pairs = content.split("\001");
		for (String pair : pairs) {
			int delimiterIndex = pair.indexOf('\002');
			if (delimiterIndex >= 0) {
				String key = pair.substring(0, delimiterIndex);
				String value = pair.substring(delimiterIndex + 1);
				rowMap.put(key, value);
			}
		}

		for (int c = 0; c < numColumns; c++) {
			String columnName = columnNames.get(c);
			TypeInfo typeInfo = columnTypes.get(c);
			if (typeInfo instanceof PrimitiveTypeInfo) {
				PrimitiveTypeInfo pti = (PrimitiveTypeInfo) typeInfo;
				String t = rowMap.get(columnName).toString();

				switch (pti.getPrimitiveCategory()) {
				case STRING:
					row.set(c, t);
					break;
				case BYTE:
					Byte b;
					b = Byte.valueOf(t);
					row.set(c, b);
					break;
				case SHORT:
					Short s;
					s = Short.valueOf(t);
					row.set(c, s);
					break;
				case INT:
					Integer i;
					i = Integer.valueOf(t);
					row.set(c, i);
					break;
				case LONG:
					Long l;
					l = Long.valueOf(t);
					row.set(c, l);
					break;
				case FLOAT:
					Float f;
					f = Float.valueOf(t);
					row.set(c, f);
					break;
				case DOUBLE:
					Double d;
					d = Double.valueOf(t);
					row.set(c, d);
					break;
				case BOOLEAN:
					Boolean bool;
					bool = Boolean.valueOf(t);
					row.set(c, bool);
					break;
				case TIMESTAMP:
					Timestamp ts;
					ts = Timestamp.valueOf(t);
					row.set(c, ts);
					break;
				case DATE:
					Date date;
					date = Date.valueOf(t);
					row.set(c, date);
					break;
				case DECIMAL:
					HiveDecimal bd = HiveDecimal.create(t);
					row.set(c, bd);
					break;
				case CHAR:
					HiveChar hc = new HiveChar(t, ((CharTypeInfo) typeInfo).getLength());
					row.set(c, hc);
					break;
				case VARCHAR:
					HiveVarchar hv = new HiveVarchar(t, ((VarcharTypeInfo) typeInfo).getLength());
					row.set(c, hv);
					break;
				default:
					throw new SerDeException("Unsupported type " + typeInfo);
				}
			}
		}

		return row;
	}

	@Override
	public Writable serialize(Object obj, ObjectInspector objectInspector) throws SerDeException {

		StructObjectInspector structOI = (StructObjectInspector) objectInspector;
		List<? extends StructField> structFields = structOI.getAllStructFieldRefs();

		if (structFields.size() != columnNames.size()) {
			throw new SerDeException("Cannot serialize this data: number of input fields must be " + columnNames.size());
		}

		StringBuilder builder = new StringBuilder();

		for (int c = 0; c < numColumns; c++) {
			if (builder.length() > 0) {
				builder.append("\001");
			}

			String columnName = columnNames.get(c);
			StructField structField = structFields.get(c);
			Object fieldData = structOI.getStructFieldData(obj, structField);

			TypeInfo typeInfo = columnTypes.get(c);
			if (typeInfo instanceof PrimitiveTypeInfo) {
				PrimitiveTypeInfo pti = (PrimitiveTypeInfo) typeInfo;
				AbstractPrimitiveJavaObjectInspector oi = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(pti);
				String t = oi.getPrimitiveJavaObject(fieldData).toString();

				switch (pti.getPrimitiveCategory()) {
				case STRING:
					row.set(c, StringUtils.repeat(t, ":", txt_multiply));
					break;
				case BYTE:
					Byte b;
					b = Byte.valueOf(t);
					row.set(c, b);
					break;
				case SHORT:
					Short s;
					s = Short.valueOf(t);
					row.set(c, s);
					break;
				case INT:
					Integer i;
					i = Integer.valueOf(t);
					row.set(c, i * num_multiply);
					break;
				case LONG:
					Long l;
					l = Long.valueOf(t);
					row.set(c, l * num_multiply);
					break;
				case FLOAT:
					Float f;
					f = Float.valueOf(t);
					row.set(c, f * num_multiply);
					break;
				case DOUBLE:
					Double d;
					d = Double.valueOf(t);
					row.set(c, d * num_multiply);
					break;
				case BOOLEAN:
					Boolean bool;
					bool = Boolean.valueOf(t);
					row.set(c, bool);
					break;
				case TIMESTAMP:
					Timestamp ts;
					ts = Timestamp.valueOf(t);
					row.set(c, ts);
					break;
				case DATE:
					Date date;
					date = Date.valueOf(t);
					row.set(c, date);
					break;
				case DECIMAL:
					HiveDecimal bd = HiveDecimal.create(t);
					row.set(c * num_multiply, bd);
					break;
				case CHAR:
					HiveChar hc = new HiveChar(StringUtils.repeat(t, ":", txt_multiply), ((CharTypeInfo) typeInfo).getLength());
					row.set(c, hc);
					break;
				case VARCHAR:
					HiveVarchar hv = new HiveVarchar(StringUtils.repeat(t, ":", txt_multiply), ((VarcharTypeInfo) typeInfo).getLength());
					row.set(c, hv);
					break;
				default:
					throw new SerDeException("Unsupported type " + typeInfo);
				}
			}

			builder.append(columnName).append("\002").append(row.get(c));
		}

		return new Text(builder.toString());
	}

	@Override
	public SerDeStats getSerDeStats() {
		// no support for statistics
		return null;
	}
}