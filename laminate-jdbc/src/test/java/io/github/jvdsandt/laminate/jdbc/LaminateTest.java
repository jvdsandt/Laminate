package io.github.jvdsandt.laminate.jdbc;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LaminateTest {

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
	void test_select_all_users() throws SQLException {
		databaseHelper.createUsersTable();
		try (Statement st = databaseHelper.createStatement();
		     ResultSet rs = st.executeQuery("SELECT * FROM users")) {
			Laminate.write(rs, Path.of("target/users.parquet"));
		}
	}

	@Test
	void test_select_all_products() throws SQLException {
		databaseHelper.createProductsTable();
		try (Statement st = databaseHelper.createStatement();
		     ResultSet rs = st.executeQuery("SELECT * FROM products")) {
			Laminate.write(rs, Path.of("target/products.parquet"));
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
}
