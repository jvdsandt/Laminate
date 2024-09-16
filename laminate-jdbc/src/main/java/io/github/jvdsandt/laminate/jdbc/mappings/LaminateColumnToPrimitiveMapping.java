package io.github.jvdsandt.laminate.jdbc.mappings;

import org.apache.parquet.schema.PrimitiveType;

public abstract class LaminateColumnToPrimitiveMapping extends LaminateColumnMapping {

	protected final PrimitiveType type;

	public LaminateColumnToPrimitiveMapping(int columnIndex, PrimitiveType type) {
		super(columnIndex);
		this.type = type;
	}

	public boolean isRequired() {
		return type.getRepetition() == PrimitiveType.Repetition.REQUIRED;
	}

	public PrimitiveType getType() {
		return type;
	}

}
