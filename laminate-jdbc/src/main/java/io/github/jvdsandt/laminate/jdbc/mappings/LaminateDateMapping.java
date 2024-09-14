package io.github.jvdsandt.laminate.jdbc.mappings;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

/**
 * Mapping class for converting a JDBC Date column to a Parquet Date field.
 */
public class LaminateDateMapping extends LaminateColumnMapping {

	public LaminateDateMapping(int columnIndex, String parqFiledName, boolean isRequired) {
		super(columnIndex, parqFiledName, isRequired);
	}

	@Override
	public void addField(Types.MessageTypeBuilder builder) {
		Type.Repetition repetition = isRequired ? Type.Repetition.REQUIRED : Type.Repetition.OPTIONAL;
		builder.primitive(primitiveType(), repetition)
				.as(LogicalTypeAnnotation.dateType())
				.named(parqFieldName);
	}

	@Override
	public PrimitiveType.PrimitiveTypeName primitiveType() {
		return PrimitiveType.PrimitiveTypeName.INT32;
	}

	@Override
	public void write(ResultSet rs, RecordConsumer rc) throws SQLException {
		Date value = rs.getDate(columnIndex);
		if (value != null) {
			rc.startField(parqFieldName, columnIndex-1);
			rc.addInteger((int) value.toLocalDate().toEpochDay());
			rc.endField(parqFieldName, columnIndex-1);
		}
	}
}
