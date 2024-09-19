package io.github.jvdsandt.laminate.jdbc;

import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;

import io.github.jvdsandt.laminate.jdbc.mappings.LaminateGroupMapping;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MultipleTableTest {

	private DatabaseHelper databaseHelper;
	private Statement statement;

	@BeforeEach
	void setUp() throws SQLException {
		databaseHelper = new DatabaseHelper();
		statement = databaseHelper.createStatement();
		setupTables();
	}

	@AfterEach
	void tearDown() throws SQLException {
		if (statement != null) {
			statement.close();
			statement = null;
		}
		if (databaseHelper != null) {
			databaseHelper.close();
			databaseHelper = null;
		}
	}

	@Test
	void test() throws Exception {
		var orderSql = "SELECT * FROM orders ORDER BY id";
		var linesSql = "SELECT * FROM order_lines ORDER BY order_id, line_nr";

		try (ResultSet rs = statement.executeQuery(orderSql);
			Statement linesStatement = databaseHelper.createStatement();
			ResultSet linesRs = linesStatement.executeQuery(linesSql)) {

			LaminateGroupMapping lineMapping = new LaminateMappingBuilder()
					.initFrom(linesRs.getMetaData())
					.withMessageTypeName("Lines")
					.build();
			LaminateGroupMapping orderMapping = new LaminateMappingBuilder()
					.initFrom(rs.getMetaData())
					.withMessageTypeName("Orders")
					.addSubgroupMapping(rs.getMetaData(), "id", linesRs.getMetaData(), "order_id", "lines", lineMapping)
					.build();

			Path output = Path.of("target/orders.parquet");
			try (ParquetWriter<ResultSet> writer = new LaminateParquetWriterBuilder(output, orderMapping)
					.withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
					.withValidation(true)
					.build()) {
				while (rs.next()) {
					writer.write(rs);
				}
			}
		}
	}

	private void setupTables() throws SQLException {
		statement.execute("""
			CREATE TABLE orders (
				id INT PRIMARY KEY, 
				orderdate date NOT NULL, 
				cust_name VARCHAR(255) NOT NULL, 
				status varchar(20) NOT NULL)
		""");
		statement.execute("""
			CREATE TABLE order_lines (
				order_id INT NOT NULL,
				line_nr INT NOT NULL,
				product VARCHAR(255) NOT NULL,
				amount INT NOT NULL,
				price NUMERIC(10,2) NOT NULL,
				PRIMARY KEY (order_id, line_nr))
		""");
		try (PreparedStatement pso = databaseHelper.prepareStatement("INSERT INTO orders VALUES (?, ?, ?, ?)");
		     PreparedStatement psl = databaseHelper.prepareStatement("INSERT INTO order_lines VALUES (?, ?, ?, ?, ?)")) {
			for (int i = 0; i < 100; i++) {
				pso.setInt(1, 1000 + i);
				pso.setObject(2, LocalDate.of(2024, 1, 1).plusDays(i));
				pso.setString(3, "Cust #" + i);
				pso.setString(4, "NEW");
				pso.executeUpdate();
				int lines = i % 4;
				for (int lnr = 0; lnr < lines; lnr++) {
					psl.setInt(1, 1000 + i);
					psl.setInt(2, lnr);
					psl.setString(3, "Product #" + lnr);
					psl.setInt(4, 2);
					psl.setBigDecimal(5, java.math.BigDecimal.valueOf(10.0 + lnr));
					psl.executeUpdate();
				}
			}
		}
	}
}
