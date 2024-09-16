package io.github.jvdsandt.laminate.jdbc.mappings;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

public class LaminateTimestampMapping extends LaminateColumnToPrimitiveMapping {

	private final LogicalTypeAnnotation.TimestampLogicalTypeAnnotation typeAnnotation;

	public LaminateTimestampMapping(int columnIndex, PrimitiveType type) {
		super(columnIndex, type);
		this.typeAnnotation = (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
	}

	@Override
	public void write(ResultSet rs, RecordConsumer rc) throws SQLException {
		Timestamp value = rs.getTimestamp(columnIndex);
		if (!rs.wasNull()) {
			rc.startField(type.getName(), fieldIndex);
			rc.addLong(value.getTime());
			rc.endField(type.getName(), fieldIndex);
		}
	}
}
