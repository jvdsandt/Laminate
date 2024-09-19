package io.github.jvdsandt.laminate.jdbc.mappings;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.Type;

public class LaminateColumnToGroupMapping extends LaminateColumnMapping {

	private final LaminateGroupMapping groupMapping;

	public LaminateColumnToGroupMapping(int columnIndex, LaminateGroupMapping groupMapping) {
		super(columnIndex);
		this.groupMapping = groupMapping;
	}

	@Override
	public Type getType() {
		return groupMapping.toMessageType();
	}

	@Override
	public void write(ResultSet rs, RecordConsumer rc) throws SQLException {

	}
}
