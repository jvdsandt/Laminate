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
import io.github.jvdsandt.laminate.jdbc.mappings.LaminateStringMapping;
import io.github.jvdsandt.laminate.jdbc.mappings.LaminateTimestampMapping;

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
		var parqFieldName = metaData.getColumnName(index).toLowerCase(Locale.ROOT);
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
					withMapping(new LaminateBigDecimalMapping(index, parqFieldName, isRequired, precision, scale));
				} else {
					withMapping(new LaminateDoubleMapping(index, parqFieldName, isRequired));
				}
				break;
			case Types.DOUBLE:
				withMapping(new LaminateDoubleMapping(index, parqFieldName, isRequired));
				break;
			case Types.BOOLEAN, Types.BIT:
				addBooleanMapping(index, parqFieldName, isRequired);
				break;
			case Types.VARCHAR, Types.CHAR, Types.CLOB, Types.LONGVARCHAR:
				withMapping(new LaminateStringMapping(index, parqFieldName, isRequired));
				break;
			case Types.DATE:
				withMapping(new LaminateDateMapping(index, parqFieldName, isRequired));
				break;
			case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE:
				withMapping(new LaminateTimestampMapping(index, parqFieldName, isRequired));
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
		return withMapping(new LaminateIntMapping(columnIndex, parqFieldName, isRequired));
	}

	public LaminateMappingBuilder addLongMapping(int columnIndex, String parqFieldName, boolean isRequired) {
		return withMapping(new LaminateLongMapping(columnIndex, parqFieldName, isRequired));
	}

	public LaminateMappingBuilder addBooleanMapping(int columnIndex, String parqFieldName, boolean isRequired) {
		return withMapping(new LaminateBooleanMapping(columnIndex, parqFieldName, isRequired));
	}

	public LaminateMappingBuilder addStringMapping(int columnIndex, String parqFieldName, boolean isRequired) {
		return withMapping(new LaminateStringMapping(columnIndex, parqFieldName, isRequired));
	}

	public LaminateGroupMapping build() {
		return new LaminateGroupMapping(mappings.toArray(new LaminateColumnMapping[0]), messageTypeName);
	}
}
