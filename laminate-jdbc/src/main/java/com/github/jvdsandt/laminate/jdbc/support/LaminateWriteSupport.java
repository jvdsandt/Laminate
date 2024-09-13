package com.github.jvdsandt.laminate.jdbc.support;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.github.jvdsandt.laminate.jdbc.mappings.LaminateGroupMapping;
import com.github.jvdsandt.laminate.jdbc.mappings.LaminateJdbcMapping;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;

public class LaminateWriteSupport extends WriteSupport<ResultSet> {

	private final LaminateGroupMapping groupMapping;
	private final Map<String, String> extraMetaData;

	private RecordConsumer recordConsumer;

	public LaminateWriteSupport(LaminateGroupMapping groupMapping, Map<String, String> extraMetaData) {
		this.groupMapping = groupMapping;
		this.extraMetaData = extraMetaData;
	}

	@Override
	public WriteContext init(ParquetConfiguration configuration) {
		return new WriteContext(groupMapping.toMessageType(), extraMetaData);
	}

	@Override
	public WriteContext init(Configuration configuration) {
		return new WriteContext(groupMapping.toMessageType(), extraMetaData);
	}

	@Override
	public void prepareForWrite(RecordConsumer recordConsumer) {
		this.recordConsumer = recordConsumer;
	}

	@Override
	public void write(ResultSet rs) {
		try {
			recordConsumer.startMessage();
			for (LaminateJdbcMapping m : groupMapping.mappings()) {
				m.write(rs, recordConsumer);
			}
			recordConsumer.endMessage();
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}
}
