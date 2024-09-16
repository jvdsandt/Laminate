package io.github.jvdsandt.laminate.jdbc.mappings;

/**
 * An abstract mapping class for converting a JDBC column to a Parquet field.
 * Dependening on the columnType a specific subclass should be used.
 */
public abstract class LaminateColumnMapping extends LaminateJdbcMapping {

	protected final int columnIndex;
	protected int fieldIndex;

	public LaminateColumnMapping(int columnIndex) {
		this.columnIndex = columnIndex;
	}

	void setFieldIndex(int newValue) {
		this.fieldIndex = newValue;
	}
}
