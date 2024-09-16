package io.github.jvdsandt.laminate.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
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
	void test_select_all_users() throws Exception {
		databaseHelper.createUsersTable();
		Path path = Path.of("target/users.parquet");
		try (Statement st = databaseHelper.createStatement();
		     ResultSet rs = st.executeQuery("SELECT * FROM users")) {
			Laminate.write(rs, path);
		}
		MessageType schema = ParquetHelper.getMessageType(path);
		assertEquals(2, schema.getFieldCount());
		List<Group> data = ParquetHelper.readData(path);
		assertEquals(5, data.size());
		Group firstRecord = data.get(0);
		assertEquals(111, firstRecord.getInteger("id", 0));
		assertEquals("Alice", firstRecord.getString("name", 0));
	}

	@Test
	void test_select_all_products() throws Exception {
		databaseHelper.createProductsTable();
		Path path = Path.of("target/products.parquet");
		try (Statement st = databaseHelper.createStatement();
		     ResultSet rs = st.executeQuery("SELECT * FROM products")) {
			Laminate.write(rs, path);
		}
		MessageType schema = ParquetHelper.getMessageType(path);
		assertEquals(6, schema.getFieldCount());
		assertEquals(Type.Repetition.REQUIRED, schema.getType("id").getRepetition());
		assertEquals(PrimitiveType.PrimitiveTypeName.INT64, schema.getType("id").asPrimitiveType().getPrimitiveTypeName());
	}

}
