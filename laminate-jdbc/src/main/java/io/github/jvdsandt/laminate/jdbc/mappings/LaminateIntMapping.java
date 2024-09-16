package io.github.jvdsandt.laminate.jdbc.mappings;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.PrimitiveType;

public class LaminateIntMapping extends LaminateColumnToPrimitiveMapping {

	public LaminateIntMapping(int columnIndex, PrimitiveType type) {
		super(columnIndex, type);
	}

	@Override
	public void write(ResultSet rs, RecordConsumer rc) throws SQLException {
		int value = rs.getInt(columnIndex);
		if (isRequired() || !rs.wasNull()) {
			rc.startField(type.getName(), fieldIndex);
			rc.addInteger(value);
			rc.endField(type.getName(), fieldIndex);
		}
	}
}
