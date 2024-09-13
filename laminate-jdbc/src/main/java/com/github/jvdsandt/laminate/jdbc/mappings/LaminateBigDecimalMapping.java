package com.github.jvdsandt.laminate.jdbc.mappings;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

public class LaminateBigDecimalMapping extends LaminateColumnMapping {

	private final int precision;
	private final int scale;
	private final BigDecimal factor;

	public LaminateBigDecimalMapping(int columnIndex, String parqFiledName, boolean isRequired, int precision, int scale) {
		super(columnIndex, parqFiledName, isRequired);
		this.precision = precision;
		this.scale = scale;
		this.factor = BigDecimal.TEN.pow(scale);
	}

	@Override
	public void write(ResultSet rs, RecordConsumer rc) throws SQLException {
		BigDecimal value = rs.getBigDecimal(columnIndex);
		if (value != null) {
			rc.startField(parqFieldName, columnIndex - 1);
			if (storeAsInt32()) {
				rc.addInteger(value.multiply(factor).intValue());
			} else if (storeAsInt64()) {
				rc.addLong(value.multiply(factor).longValue());
			} else {
				throw new RuntimeException("to do");
			}
			rc.endField(parqFieldName, columnIndex - 1);
		}
	}

	@Override
	public void addField(Types.MessageTypeBuilder builder) {
		Type.Repetition repetition = isRequired ? Type.Repetition.REQUIRED : Type.Repetition.OPTIONAL;
		builder.primitive(primitiveType(), repetition).as(LogicalTypeAnnotation.decimalType(scale, precision)).named(parqFieldName);
	}

	@Override
	public PrimitiveType.PrimitiveTypeName primitiveType() {
		if (storeAsInt32()) {
			return PrimitiveType.PrimitiveTypeName.INT32;
		} else if (storeAsInt64()) {
			return PrimitiveType.PrimitiveTypeName.INT64;
		}
		return PrimitiveType.PrimitiveTypeName.BINARY;
	}

	private boolean storeAsInt32() {
		return precision <= 9;
	}

	private boolean storeAsInt64() {
		return precision <= 18;
	}

}
