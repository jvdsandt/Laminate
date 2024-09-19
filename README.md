[![Build Status](https://github.com/jvdsandt/Laminate/actions/workflows/maven-build.yml/badge.svg)](https://github.com/jvdsandt/Laminate/actions)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.jvdsandt.laminate/laminate.svg)](https://repo1.maven.org/maven2/io/github/jvdsandt/laminate/laminate)
[![License](https://img.shields.io/:license-mit-blue.svg)](https://opensource.org/license/mit)

# Laminate
A Java library to export JDBC ResultSet data to Parquet files

## Features
- Export JDBC ResultSet data to Parquet files
- Automatically generate a Parquet schema based on the ResultSet metadata
- Optionally customize the mapping and Parquet schema for custom conversions or to support advanced JDBC datatypes like arrays

## Installation

Add this library to your Java project using Maven:

```xml
<dependency>
  <groupId>io.github.jvdsandt.laminate</groupId>
  <artifactId>laminate-jdbc</artifactId>
  <version>0.1.9</version>
</dependency>
```

### Sample application
```java
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import io.github.jvdsandt.laminate.jdbc.Laminate;
import io.github.jvdsandt.laminate.jdbc.mappings.LaminateGroupMapping;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;

public class Demo {

	public static void main(String[] args) throws SQLException {
		String query = "select * from my_table";
		try (Connection conn = createConnection(args)) {
			try (ResultSet rs = conn.createStatement().executeQuery(query)) {
				LaminateGroupMapping mapping = Laminate.mappingBuilder()
                        .initFrom(rs.getMetaData())
                        .build();
				OutputFile output = new LocalOutputFile(Path.of("my_data.parquet"));
				Laminate.write(rs, output , mapping);
			}
		}
	}

	private static Connection createConnection(String[] args) throws SQLException {
		return DriverManager.getConnection("jdbc:postgresql://localhost/postgres"
				, "demo", "demo");
	}
}
```
See the class [ApiTest](https://github.com/jvdsandt/Laminate/blob/main/laminate-jdbc/src/test/java/io/github/jvdsandt/laminate/jdbc/ApiTest.java) for more examples.