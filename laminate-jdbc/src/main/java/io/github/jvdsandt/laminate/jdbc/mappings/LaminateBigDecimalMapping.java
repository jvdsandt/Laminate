package io.github.jvdsandt.laminate.jdbc.mappings;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

public class LaminateBigDecimalMapping extends LaminateColumnToPrimitiveMapping {

	private final LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalTypeAnnotation;
	private final BigDecimal factor;

	public LaminateBigDecimalMapping(int columnIndex, PrimitiveType type) {
		super(columnIndex, type);
		this.decimalTypeAnnotation = (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
		this.factor = BigDecimal.TEN.pow(decimalTypeAnnotation.getScale());
	}

	@Override
	public void write(ResultSet rs, RecordConsumer rc) throws SQLException {
		BigDecimal value = rs.getBigDecimal(columnIndex);
		if (value != null) {
			rc.startField(type.getName(), fieldIndex);
			if (storeAsInt32()) {
				rc.addInteger(value.multiply(factor).intValue());
			} else if (storeAsInt64()) {
				rc.addLong(value.multiply(factor).longValue());
			} else {
				throw new RuntimeException("to do");
			}
			rc.endField(type.getName(), fieldIndex);
		}
	}

	private boolean storeAsInt32() {
		return decimalTypeAnnotation.getPrecision() <= 9;
	}

	private boolean storeAsInt64() {
		return decimalTypeAnnotation.getPrecision() <= 18;
	}
}
