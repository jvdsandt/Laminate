package io.github.jvdsandt.laminate.samples;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import io.github.jvdsandt.laminate.jdbc.LaminateMappingBuilder;
import io.github.jvdsandt.laminate.jdbc.LaminateParquetWriterBuilder;
import io.github.jvdsandt.laminate.jdbc.mappings.LaminateGroupMapping;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;

public class PostgresTests {

	@Container
	static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15.8");

	private static Connection connection;

	@BeforeAll
	static void setUp() throws Exception {
		postgres.start();
		connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
	}

	@AfterAll
	static void tearDown() throws Exception {
		connection.close();
		postgres.stop();
	}

	@BeforeEach
	void setupEach() throws Exception{
		try (Statement statement = connection.createStatement()) {
			statement.execute("DROP TABLE IF EXISTS test");
		}
	}

	@Test
	void test_simple_export() throws Exception {
		try (Statement statement = connection.createStatement()) {
			statement.execute("CREATE TABLE test (id SERIAL PRIMARY KEY, name VARCHAR(255))");
			statement.execute("INSERT INTO test (name) VALUES ('firstname')");
			statement.execute("INSERT INTO test (name) VALUES ('secondname')");
			ResultSet rs = statement.executeQuery("SELECT id, name FROM test");
			LaminateGroupMapping mapping = new LaminateMappingBuilder()
					.initFrom(rs.getMetaData())
					.withMessageTypeName("Test")
					.build();
			Path output = Path.of("target/pg_simple.parquet");
			try (ParquetWriter<ResultSet> writer = new LaminateParquetWriterBuilder(output, mapping)
					.withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
					.withValidation(true)
					.build()) {
				while (rs.next()) {
					writer.write(rs);
				}
			}
		}
	}

	@Test
	void test_date_export() throws Exception {
		try (Statement statement = connection.createStatement()) {
			statement.execute("CREATE TABLE test (id SERIAL PRIMARY KEY, startdate DATE)");
			statement.execute("INSERT INTO test (startdate) VALUES ('2024-09-20')");
			statement.execute("INSERT INTO test (startdate) VALUES ('2030-12-13')");
			ResultSet rs = statement.executeQuery("SELECT id, startdate FROM test");
			LaminateGroupMapping mapping = new LaminateMappingBuilder()
					.initFrom(rs.getMetaData())
					.withMessageTypeName("Test")
					.build();
			Path output = Path.of("target/pg_simple.parquet");
			try (ParquetWriter<ResultSet> writer = new LaminateParquetWriterBuilder(output, mapping)
					.withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
					.withValidation(true)
					.build()) {
				while (rs.next()) {
					writer.write(rs);
				}
			}
		}
	}
}
