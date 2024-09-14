package io.github.jvdsandt.laminate.jdbc.mappings;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.parquet.io.api.RecordConsumer;

/**
 * An abstract mapping class for converting JDBC data Parquet data.
 */
public abstract class LaminateJdbcMapping {

	public abstract void write(ResultSet rs, RecordConsumer rc) throws SQLException;

}
