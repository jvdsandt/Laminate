package io.github.jvdsandt.laminate.jdbc;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import io.github.jvdsandt.laminate.jdbc.mappings.LaminateGroupMapping;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ApiTest {

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
  void test_with_auto_mapping() throws Exception {
    databaseHelper.createUsersTable();
    try (Statement st = databaseHelper.createStatement();
         ResultSet rs = st.executeQuery("SELECT * FROM users")) {
      LaminateGroupMapping mapping = new LaminateMappingBuilder()
              .initFrom(rs.getMetaData())
              .withMessageTypeName("Users")
              .build();
      Path output = Path.of("users.parquet");
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
  void test_with_custom_mapping() throws Exception {
    databaseHelper.createUsersTable();
    try (Statement st = databaseHelper.createStatement();
         ResultSet rs = st.executeQuery("SELECT id, name FROM users")) {
      LaminateGroupMapping mapping = new LaminateMappingBuilder()
              .addIntMapping(1, "id", true)
              .addStringMapping(2, "name", false)
              .withMessageTypeName("Users")
              .build();
      Path output = Path.of("users.parquet");
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
