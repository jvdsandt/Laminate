package com.github.jvdsandt.laminate.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseHelper {

	private Connection connection;

	public DatabaseHelper() throws SQLException {
		this.connection = DriverManager.getConnection("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
	}

	public void createUsersTable() throws SQLException {
		try (Statement stmt = connection.createStatement()) {
			stmt.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255))");
			stmt.execute("INSERT INTO users (id, name) VALUES (111, 'Alice'), (222, 'Bob'), (333, null), (444, ''), (555, 'Diana')");
		}
	}

	public void createProductsTable() throws SQLException {
		try (Statement stmt = connection.createStatement()) {
			stmt.execute("""
				CREATE TABLE products (
    				id BIGINT PRIMARY KEY,
					created_at TIMESTAMP NOT NULL,
					valid_from DATE,
					name VARCHAR(255) NOT NULL,
					price numeric(10, 2),
					description clob) 	
				""");
		}
		var insertSql = "INSERT INTO products (id, created_at, valid_from, name, price, description) VALUES (?, ?, ?, ?, ?, ?)";
		try (PreparedStatement ps = connection.prepareStatement(insertSql)) {
			for (int i = 0; i < 100; i++) {
				ps.setLong(1, i * 1000);
				ps.setTimestamp(2, new java.sql.Timestamp(System.currentTimeMillis()));
				ps.setDate(3, new java.sql.Date(System.currentTimeMillis()));
				ps.setString(4, "Product #" + i);
				ps.setBigDecimal(5, java.math.BigDecimal.valueOf(i * 10.12345));
				ps.setString(6, " Description #" + i);
				ps.executeUpdate();
			}
		}
	}

	public void close() throws SQLException {
		if (this.connection != null) {
			this.connection.close();
			this.connection = null;
		}
	}

	public Connection getConnection() {
		return connection;
	}
}
