package com.github.jvdsandt.laminate.jdbc;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.github.jvdsandt.laminate.jdbc.mappings.LaminateGroupMapping;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;

/**
 * Laminate is a utility class for writing JDBC ResultSet data to Parquet files.
 */
public class Laminate {

	public static void write(ResultSet rs, Path path) throws SQLException {
		write(rs, new LocalOutputFile(path));
	}

	/**
	 * Answer a LaminateMappingBuilder that is initialized with the metadata from the ResultSet.
	 */
	public static LaminateMappingBuilder mappingBuilder(ResultSet rs) throws SQLException {
		return new LaminateMappingBuilder(rs.getMetaData()).init();
	}

	/**
	 * Write the ResultSet to the outputFile. A default mapping is created using the ResultSetMetaData.
	 */
	public static void write(ResultSet rs, OutputFile outputFile)  {
		try {
			write(rs, outputFile, mappingBuilder(rs).build());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Write the ResultSet to the outputFile using the specified mapping.
	 */
	public static void write(ResultSet rs, OutputFile outputFile, LaminateGroupMapping mapping) {
		try (ParquetWriter<ResultSet> writer = new LaminateParquetWriterBuilder(outputFile, mapping)
				.withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
				.withValidation(true)
				.build()) {
			write(rs, writer);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static void write(ResultSet rs, ParquetWriter<ResultSet> writer) throws SQLException, IOException {
		while (rs.next()) {
			writer.write(rs);
		}
	}

	private Laminate() {
	}

}
