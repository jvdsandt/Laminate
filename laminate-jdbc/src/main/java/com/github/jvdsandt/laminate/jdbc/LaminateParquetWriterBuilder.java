package com.github.jvdsandt.laminate.jdbc;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import com.github.jvdsandt.laminate.jdbc.mappings.LaminateGroupMapping;
import com.github.jvdsandt.laminate.jdbc.support.LaminateWriteSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;

public class LaminateParquetWriterBuilder extends ParquetWriter.Builder<ResultSet, LaminateParquetWriterBuilder> {

	private LaminateGroupMapping groupMapping;
	private Map<String, String> extraMetaData = new HashMap<>();

	public LaminateParquetWriterBuilder(OutputFile path, LaminateGroupMapping groupMapping) {
		super(path);
		this.groupMapping = groupMapping;
	}

	@Override
	public LaminateParquetWriterBuilder withExtraMetaData(Map<String, String> extraMetaData) {
		this.extraMetaData.putAll(extraMetaData);
		return this;
	}

	public LaminateParquetWriterBuilder withExtraMetaData(String key, String value) {
		this.extraMetaData.put(key, value);
		return this;
	}

	@Override
	protected LaminateParquetWriterBuilder self() {
		return this;
	}

	@Override
	protected WriteSupport<ResultSet> getWriteSupport(ParquetConfiguration conf) {
		return getWriteSupport();
	}

	@Override
	protected WriteSupport<ResultSet> getWriteSupport(Configuration conf) {
		return getWriteSupport();
	}

	private WriteSupport<ResultSet> getWriteSupport() {
		return new LaminateWriteSupport(groupMapping, extraMetaData);
	}
}
