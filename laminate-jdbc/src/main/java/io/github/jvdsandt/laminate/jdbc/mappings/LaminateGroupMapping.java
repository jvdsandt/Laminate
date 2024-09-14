package io.github.jvdsandt.laminate.jdbc.mappings;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import io.github.jvdsandt.laminate.jdbc.LaminateMappingBuilder;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that contains the required mappings to convert a JDBC ResultSet to a Parquet file.
 * It can generate the Parquet GroupType or MessageType object, and it knows how to write
 * the data from the JDBC ResultSet to a Parquet file.
 *
 * It can be initialized from a JDBC ResultSetMetaData object. If needed, the column mappings
 * can be customized.
 */
public class LaminateGroupMapping {

	private static final Logger LOG = LoggerFactory.getLogger(LaminateGroupMapping.class);

	private final LaminateColumnMapping[] mappings;
	private final String messageTypeName;

	public static LaminateGroupMapping buildFrom(ResultSetMetaData metaData) throws SQLException {
		return new LaminateMappingBuilder(metaData)
				.init()
				.build();
	}

	public LaminateGroupMapping(LaminateColumnMapping[] mappings, String messageTypeName) {
		this.mappings = mappings;
		this.messageTypeName = messageTypeName;
	}

	public MessageType toMessageType() {
		org.apache.parquet.schema.Types.MessageTypeBuilder builder = org.apache.parquet.schema.Types.buildMessage();
		for (var mapping : mappings) {
			mapping.addField(builder);
		}
		MessageType msgType = builder.named(messageTypeName);
		LOG.info("Generated schema: {}", msgType);
		return msgType;
	}

	public LaminateJdbcMapping[] mappings() {
		return mappings;
	}

}
