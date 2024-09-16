package io.github.jvdsandt.laminate.jdbc.mappings;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.PrimitiveType;

/**
 * Mapping class for converting a JDBC Date column to a Parquet Date field.
 */
public class LaminateDateMapping extends LaminateColumnToPrimitiveMapping {

	public LaminateDateMapping(int columnIndex, PrimitiveType type) {
		super(columnIndex, type);
	}

	@Override
	public void write(ResultSet rs, RecordConsumer rc) throws SQLException {
		LocalDate value = rs.getObject(columnIndex, LocalDate.class);
		if (value != null) {
			rc.startField(type.getName(), fieldIndex);
			rc.addInteger((int) value.toEpochDay());
			rc.endField(type.getName(), fieldIndex);
		}
	}
}
