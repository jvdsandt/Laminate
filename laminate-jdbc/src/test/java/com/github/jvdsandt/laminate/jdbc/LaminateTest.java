package com.github.jvdsandt.laminate.jdbc;

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
		try (Statement st = databaseHelper.getConnection().createStatement();
		     ResultSet rs = st.executeQuery("SELECT * FROM users")) {
			Laminate.write(rs, Path.of("target/users.parquet"));
		}
	}

	@Test
	void test_select_all_products() throws SQLException {
		databaseHelper.createProductsTable();
		try (Statement st = databaseHelper.getConnection().createStatement();
		     ResultSet rs = st.executeQuery("SELECT * FROM products")) {
			Laminate.write(rs, Path.of("target/products.parquet"));
		}
	}
}
