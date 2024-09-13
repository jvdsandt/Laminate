package com.github.jvdsandt.laminate.jdbc.mappings;

import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

/**
 * An abstract mapping class for converting a JDBC column to a Parquet field.
 * Dependening on the columnType a specific subclass should be used.
 */
public abstract class LaminateColumnMapping extends LaminateJdbcMapping {

	protected final int columnIndex;

	protected final boolean isRequired;
	protected String parqFieldName;

	public LaminateColumnMapping(int columnIndex, String parqFieldName, boolean isRequired) {
		this.columnIndex = columnIndex;
		this.parqFieldName = parqFieldName;
		this.isRequired = isRequired;
	}

	public void addField(Types.MessageTypeBuilder builder) {
		Type.Repetition repetition = isRequired ? Type.Repetition.REQUIRED : Type.Repetition.OPTIONAL;
		builder.primitive(primitiveType(), repetition).named(parqFieldName);
	}

	public abstract PrimitiveType.PrimitiveTypeName primitiveType();
}
