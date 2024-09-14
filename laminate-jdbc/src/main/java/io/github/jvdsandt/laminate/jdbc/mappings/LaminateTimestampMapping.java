package io.github.jvdsandt.laminate.jdbc.mappings;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

public class LaminateTimestampMapping extends LaminateColumnMapping {

	public LaminateTimestampMapping(int columnIndex, String parqFiledName, boolean isRequired) {
		super(columnIndex, parqFiledName, isRequired);
	}

	@Override
	public void addField(Types.MessageTypeBuilder builder) {
		Type.Repetition repetition = isRequired ? Type.Repetition.REQUIRED : Type.Repetition.OPTIONAL;
		builder.primitive(primitiveType(), repetition)
				.as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
				.named(parqFieldName);
	}

	@Override
	public PrimitiveType.PrimitiveTypeName primitiveType() {
		return PrimitiveType.PrimitiveTypeName.INT64;
	}

	@Override
	public void write(ResultSet rs, RecordConsumer rc) throws SQLException {
		Timestamp value = rs.getTimestamp(columnIndex);
		if (!rs.wasNull()) {
			rc.startField(parqFieldName, columnIndex-1);
			rc.addLong(value.getTime());
			rc.endField(parqFieldName, columnIndex-1);
		}
	}
}
