package io.github.jvdsandt.laminate.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import io.github.jvdsandt.laminate.jdbc.mappings.LaminateBigDecimalMapping;
import io.github.jvdsandt.laminate.jdbc.mappings.LaminateBooleanMapping;
import io.github.jvdsandt.laminate.jdbc.mappings.LaminateColumnMapping;
import io.github.jvdsandt.laminate.jdbc.mappings.LaminateDateMapping;
import io.github.jvdsandt.laminate.jdbc.mappings.LaminateDoubleMapping;
import io.github.jvdsandt.laminate.jdbc.mappings.LaminateGroupMapping;
import io.github.jvdsandt.laminate.jdbc.mappings.LaminateIntMapping;
import io.github.jvdsandt.laminate.jdbc.mappings.LaminateLongMapping;
import io.github.jvdsandt.laminate.jdbc.mappings.LaminateStringArrayMapping;
import io.github.jvdsandt.laminate.jdbc.mappings.LaminateStringMapping;
import io.github.jvdsandt.laminate.jdbc.mappings.LaminateTimestampMapping;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * Builder class for creating LaminateGroupMapping objects from JDBC ResultSetMetaData.
 */
public class LaminateMappingBuilder {

	private final List<LaminateColumnMapping> mappings = new ArrayList<>();
	private String messageTypeName = "Record";

	public LaminateMappingBuilder() {
		super();
	}

	public LaminateMappingBuilder(ResultSetMetaData metaData) throws SQLException {
		this();
		initFrom(metaData);
	}

	public LaminateMappingBuilder initFrom(ResultSetMetaData metaData) throws SQLException {
		for (int i = 1; i <= metaData.getColumnCount(); i++) {
			initFromColumn(metaData, i);
		}
		return this;
	}

	private void initFromColumn(ResultSetMetaData metaData, int index) throws SQLException {
		var sqlType = metaData.getColumnType(index);
		var isRequired = metaData.isNullable(index) == ResultSetMetaData.columnNoNulls;
		var parqFieldName = columnNameToFieldName(metaData.getColumnName(index));
		switch (sqlType) {
			case Types.INTEGER, Types.SMALLINT, Types.TINYINT:
				addIntMapping(index, parqFieldName, isRequired);
				break;
			case Types.BIGINT:
				addLongMapping(index, parqFieldName, isRequired);
				break;
			case Types.NUMERIC, Types.DECIMAL:
				int precision = metaData.getPrecision(index);
				int scale = metaData.getScale(index);
				if (precision > 0) {
					addBigDecimalMapping(index, parqFieldName, isRequired, precision, scale);
				} else {
					addDoubleMapping(index, parqFieldName, isRequired);
				}
				break;
			case Types.DOUBLE:
				addDoubleMapping(index, parqFieldName, isRequired);
				break;
			case Types.BOOLEAN:
				addBooleanMapping(index, parqFieldName, isRequired);
				break;
			case Types.VARCHAR, Types.CHAR, Types.CLOB, Types.LONGVARCHAR:
				addStringMapping(index, parqFieldName, isRequired);
				break;
			case Types.DATE:
				addDateMapping(index, parqFieldName, isRequired);
				break;
			case Types.TIMESTAMP:
				addTimestampMapping(index, parqFieldName, isRequired, false);
				break;
			case Types.TIMESTAMP_WITH_TIMEZONE:
				addTimestampMapping(index, parqFieldName, isRequired, true);
				break;
			default:
				throw new RuntimeException("Unsupported SQL type: " + sqlType + " name: " + metaData.getColumnTypeName(index));
		}
	}

	public LaminateMappingBuilder withMapping(LaminateColumnMapping mapping) {
		mappings.add(mapping);
		return this;
	}

	public LaminateMappingBuilder withMessageTypeName(String input) {
		messageTypeName = input;
		return this;
	}

	public LaminateMappingBuilder addIntMapping(int columnIndex, String parqFieldName, boolean isRequired) {
		PrimitiveType type = primitiveBuilder(PrimitiveType.PrimitiveTypeName.INT32, isRequired).named(parqFieldName);
		return withMapping(new LaminateIntMapping(columnIndex, type));
	}

	public LaminateMappingBuilder addLongMapping(int columnIndex, String parqFieldName, boolean isRequired) {
		PrimitiveType type = primitiveBuilder(PrimitiveType.PrimitiveTypeName.INT64, isRequired).named(parqFieldName);
		return withMapping(new LaminateLongMapping(columnIndex, type));
	}

	public LaminateMappingBuilder addBooleanMapping(int columnIndex, String parqFieldName, boolean isRequired) {
		PrimitiveType type = primitiveBuilder(PrimitiveType.PrimitiveTypeName.BOOLEAN, isRequired).named(parqFieldName);
		return withMapping(new LaminateBooleanMapping(columnIndex, type));
	}

	public LaminateMappingBuilder addDoubleMapping(int columnIndex, String parqFieldName, boolean isRequired) {
		PrimitiveType type = primitiveBuilder(PrimitiveType.PrimitiveTypeName.DOUBLE, isRequired).named(parqFieldName);
		return withMapping(new LaminateDoubleMapping(columnIndex, type));
	}

	public LaminateMappingBuilder addBigDecimalMapping(int columnIndex, String parqFieldName, boolean isRequired, int precision, int scale) {
		PrimitiveType.PrimitiveTypeName typeName = PrimitiveType.PrimitiveTypeName.BINARY;
		if (precision <= 0) {
			typeName = PrimitiveType.PrimitiveTypeName.INT32;
		} else if (precision <= 18) {
			typeName = PrimitiveType.PrimitiveTypeName.INT64;
		}
		PrimitiveType type = primitiveBuilder(typeName, isRequired)
				.as(LogicalTypeAnnotation.decimalType(scale, precision))
				.named(parqFieldName);
		return withMapping(new LaminateBigDecimalMapping(columnIndex, type));
	}

	public LaminateMappingBuilder addStringMapping(int columnIndex, String parqFieldName, boolean isRequired) {
		PrimitiveType type = primitiveBuilder(PrimitiveType.PrimitiveTypeName.BINARY, isRequired)
				.as(LogicalTypeAnnotation.stringType())
				.named(parqFieldName);
		return withMapping(new LaminateStringMapping(columnIndex, type));
	}

	public LaminateMappingBuilder addStringArrayMapping(int columnIndex, String parqFieldName) {
		PrimitiveType type = org.apache.parquet.schema.Types.repeated(PrimitiveType.PrimitiveTypeName.BINARY)
				.as(LogicalTypeAnnotation.stringType())
				.named(parqFieldName);
		return withMapping(new LaminateStringArrayMapping(columnIndex, type));
	}

	public LaminateMappingBuilder addDateMapping(int columnIndex, String parqFieldName, boolean isRequired) {
		PrimitiveType type = primitiveBuilder(PrimitiveType.PrimitiveTypeName.INT32, isRequired)
				.as(LogicalTypeAnnotation.dateType())
				.named(parqFieldName);
		return withMapping(new LaminateDateMapping(columnIndex, type));
	}

	public LaminateMappingBuilder addTimestampMapping(int columnIndex, String parqFieldName, boolean isRequired, boolean withTimezone) {
		PrimitiveType type = primitiveBuilder(PrimitiveType.PrimitiveTypeName.INT64, isRequired)
				.as(LogicalTypeAnnotation.timestampType(withTimezone, LogicalTypeAnnotation.TimeUnit.MILLIS))
				.named(parqFieldName);
		return withMapping(new LaminateTimestampMapping(columnIndex, type));
	}

	protected String columnNameToFieldName(String columnName) {
		return columnName.toLowerCase(Locale.ROOT);
	}

	public LaminateGroupMapping build() {
		return new LaminateGroupMapping(mappings.toArray(new LaminateColumnMapping[0]), messageTypeName);
	}

	private org.apache.parquet.schema.Types.PrimitiveBuilder<PrimitiveType> primitiveBuilder(PrimitiveType.PrimitiveTypeName typeName, boolean isRequired) {
		if (isRequired) {
			return org.apache.parquet.schema.Types.required(typeName);
		} else {
			return org.apache.parquet.schema.Types.optional(typeName);
		}
	}
}
