package io.github.jvdsandt.laminate.jdbc.mappings;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.PrimitiveType;

public class LaminateStringArrayMapping extends LaminateColumnToPrimitiveMapping {

	public LaminateStringArrayMapping(int columnIndex, PrimitiveType type) {
		super(columnIndex, type);
	}

	@Override
	public void write(ResultSet rs, RecordConsumer rc) throws SQLException {
		Array value = rs.getArray(columnIndex);
		if (value != null) {
			Object[] valueArray = (Object[]) value.getArray();
			if (valueArray != null && valueArray.length > 0) {
				rc.startField(type.getName(), fieldIndex);
				for (int i = 0; i < valueArray.length; i++) {
					rc.addBinary(Binary.fromString(Objects.toString(valueArray[i])));
				}
				rc.endField(type.getName(), fieldIndex);
			}
			value.free();
		}
	}
}
