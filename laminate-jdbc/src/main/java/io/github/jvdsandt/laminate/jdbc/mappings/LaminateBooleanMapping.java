package io.github.jvdsandt.laminate.jdbc.mappings;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.PrimitiveType;

public class LaminateBooleanMapping extends LaminateColumnMapping {

	public LaminateBooleanMapping(int columnIndex, String parqFiledName, boolean isRequired) {
		super(columnIndex, parqFiledName, isRequired);
	}

	@Override
	public void write(ResultSet rs, RecordConsumer rc) throws SQLException {
		boolean value = rs.getBoolean(columnIndex);
		if (isRequired || !rs.wasNull()) {
			rc.startField(parqFieldName, columnIndex-1);
			rc.addBoolean(value);
			rc.endField(parqFieldName, columnIndex-1);
		}
	}

	@Override
	public PrimitiveType.PrimitiveTypeName primitiveType() {
		return PrimitiveType.PrimitiveTypeName.BOOLEAN;
	}

}
