package com.github.jvdsandt.laminate.jdbc.mappings;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

public class LaminateStringMapping extends LaminateColumnMapping {

	public LaminateStringMapping(int columnIndex, String parqFiledName, boolean isRequired) {
		super(columnIndex, parqFiledName, isRequired);
	}

	@Override
	public void write(ResultSet rs, RecordConsumer rc) throws SQLException {
		String value = rs.getString(columnIndex);
		if (value != null) {
			rc.startField(parqFieldName, columnIndex - 1);
			rc.addBinary(Binary.fromString(value));
			rc.endField(parqFieldName, columnIndex - 1);
		}
	}

	@Override
	public void addField(Types.MessageTypeBuilder builder) {
		Type.Repetition repetition = isRequired ? Type.Repetition.REQUIRED : Type.Repetition.OPTIONAL;
		builder.primitive(primitiveType(), repetition).as(LogicalTypeAnnotation.stringType()).named(parqFieldName);
	}

	@Override
	public PrimitiveType.PrimitiveTypeName primitiveType() {
		return PrimitiveType.PrimitiveTypeName.BINARY;
	}

}
