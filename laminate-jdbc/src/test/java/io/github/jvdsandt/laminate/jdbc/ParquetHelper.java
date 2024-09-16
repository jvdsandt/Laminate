package io.github.jvdsandt.laminate.jdbc;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.MessageType;

public class ParquetHelper {

	public static MessageType getMessageType(Path path) throws IOException {
		return getMessageType(new LocalInputFile(path));
	}

	public static MessageType getMessageType(InputFile inputFile) throws IOException {
		try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
			ParquetMetadata metadata = reader.getFooter();
			return metadata.getFileMetaData().getSchema();
		}
	}

	public static List<Group> readData(Path path) throws IOException {
		return readData(new LocalInputFile(path));
	}

	public static List<Group> readData(InputFile inputFile) throws IOException {
		List<Group> records = new ArrayList<>();
		try (ParquetReader<Group> reader = new GroupReaderBuilder(inputFile).build()) {
			Group group;
			while ((group = reader.read()) != null) {
				records.add(group);
			}
		}
		return records;
	}

	static class GroupReaderBuilder extends ParquetReader.Builder<Group> {

		public GroupReaderBuilder(InputFile file) {
			super(file);
		}

		@Override
		protected ReadSupport<Group> getReadSupport() {
			return new GroupReadSupport();
		}
	}

}
