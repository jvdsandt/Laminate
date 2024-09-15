# Laminate
A Java library to export JDBC ResultSet data to Parquet files

## Usage

```xml
<dependency>
  <groupId>io.github.jvdsandt.laminate</groupId>
  <artifactId>laminate-jdbc</artifactId>
  <version>0.1.6</version>
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
