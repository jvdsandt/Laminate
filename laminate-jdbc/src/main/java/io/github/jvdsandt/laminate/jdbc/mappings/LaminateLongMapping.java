package io.github.jvdsandt.laminate.jdbc.mappings;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.PrimitiveType;

public class LaminateLongMapping extends LaminateColumnMapping {

	public LaminateLongMapping(int columnIndex, String parqFiledName, boolean isRequired) {
		super(columnIndex, parqFiledName, isRequired);
	}

	@Override
	public void write(ResultSet rs, RecordConsumer rc) throws SQLException {
		long value = rs.getLong(columnIndex);
		if (!rs.wasNull()) {
			rc.startField(parqFieldName, columnIndex-1);
			rc.addLong(value);
			rc.endField(parqFieldName, columnIndex-1);
		}
	}

	@Override
	public PrimitiveType.PrimitiveTypeName primitiveType() {
		return PrimitiveType.PrimitiveTypeName.INT64;
	}

}
