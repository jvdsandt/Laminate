package io.github.jvdsandt.laminate.jdbc;

import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import io.github.jvdsandt.laminate.jdbc.mappings.LaminateGroupMapping;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DataTypeTest {

	private DatabaseHelper databaseHelper;

	@BeforeEach
	void setUp() throws SQLException {
		databaseHelper = new DatabaseHelper();
	}

	@AfterEach
	void tearDown() throws SQLException {
		if (databaseHelper != null) {
			databaseHelper.close();
			databaseHelper = null;
		}
	}

	@Test
	void test_boolean_datatype() throws SQLException {
		try (Statement stmt = databaseHelper.createStatement()) {
			stmt.execute("CREATE TABLE users (id INT PRIMARY KEY, hasProduct BOOLEAN NOT NULL)");
			stmt.execute("INSERT INTO users (id, hasProduct) VALUES (111, true), (222, false)");
		}
		try (Statement st = databaseHelper.createStatement();
		     ResultSet rs = st.executeQuery("SELECT * FROM users")) {
			Laminate.write(rs, Path.of("target/users.parquet"));
		}
	}

	@Test
	void test_array_datatype() throws SQLException {
		try (Statement stmt = databaseHelper.createStatement()) {
			stmt.execute("CREATE TABLE dummy (id INT PRIMARY KEY, names VARCHAR(100) ARRAY)");
			stmt.execute("INSERT INTO dummy (id, names) VALUES (111, ('A', 'B', 'C')), (222, ('first','second'))");
			stmt.execute("INSERT INTO dummy (id, names) VALUES (333, ()), (444, null)");
		}
		LaminateGroupMapping mapping = new LaminateMappingBuilder()
				.addIntMapping(1, "id", true)
				.addStringArrayMapping(2, "names")
				.build();
		try (Statement st = databaseHelper.createStatement();
		     ResultSet rs = st.executeQuery("SELECT * FROM dummy")) {
			Laminate.write(rs, Path.of("target/dt_arrays.parquet"), mapping);
		}
	}

	@Test
	void test_timestamp_datatype() throws SQLException {
		try (Statement stmt = databaseHelper.createStatement()) {
			stmt.execute("CREATE TABLE dummy (id INT PRIMARY KEY, ts1 TIMESTAMP NOT NULL, ts2 TIMESTAMP WITH TIME ZONE NOT NULL)");
		}
		var quety = "INSERT INTO dummy (id, ts1, ts2) VALUES (?,?,?)";
		try (PreparedStatement ps = databaseHelper.getConnection().prepareStatement(quety)) {
				ps.setInt(1, 111);
				ps.setObject(2, LocalDateTime.of(2021, 1, 1, 12, 0, 0));
				ps.setObject(3, OffsetDateTime.of(LocalDateTime.of(2021, 1, 1, 12, 0, 0), ZoneOffset.ofHours(3)));
				ps.executeUpdate();
		}
		try (Statement st = databaseHelper.createStatement();
		     ResultSet rs = st.executeQuery("SELECT * FROM dummy")) {
			Laminate.write(rs, Path.of("target/dt_timestamps.parquet"));
		}
	}





}
