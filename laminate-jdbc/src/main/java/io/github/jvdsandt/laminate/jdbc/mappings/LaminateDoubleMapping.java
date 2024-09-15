package io.github.jvdsandt.laminate.jdbc.mappings;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.PrimitiveType;

public class LaminateDoubleMapping extends LaminateColumnMapping {

	public LaminateDoubleMapping(int columnIndex, String parqFiledName, boolean isRequired) {
		super(columnIndex, parqFiledName, isRequired);
	}

	@Override
	public void write(ResultSet rs, RecordConsumer rc) throws SQLException {
		double value = rs.getDouble(columnIndex);
		if (isRequired || !rs.wasNull()) {
			rc.startField(parqFieldName, columnIndex - 1);
			rc.addDouble(value);
			rc.endField(parqFieldName, columnIndex - 1);
		}
	}

	@Override
	public PrimitiveType.PrimitiveTypeName primitiveType() {
		return PrimitiveType.PrimitiveTypeName.DOUBLE;
	}

}
