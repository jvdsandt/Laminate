package io.github.jvdsandt.laminate.jdbc.mappings;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.PrimitiveType;

public class LaminateStringMapping extends LaminateColumnToPrimitiveMapping {

	public LaminateStringMapping(int columnIndex, PrimitiveType type) {
		super(columnIndex, type);
	}

	@Override
	public void write(ResultSet rs, RecordConsumer rc) throws SQLException {
		String value = rs.getString(columnIndex);
		if (value != null) {
			rc.startField(type.getName(), fieldIndex);
			rc.addBinary(Binary.fromString(value));
			rc.endField(type.getName(), fieldIndex);
		}
	}
}
