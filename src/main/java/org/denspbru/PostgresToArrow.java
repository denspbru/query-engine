package org.denspbru;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;

public class PostgresToArrow {
    public static Map<String, FieldVector> fromResultSet(ResultSet rs, BufferAllocator allocator) throws Exception {
        Map<String, FieldVector> columns = new HashMap<>();
        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();

        for (int i = 1; i <= colCount; i++) {
            String name = meta.getColumnLabel(i);
            int type = meta.getColumnType(i);
            switch (type) {
                case java.sql.Types.INTEGER:
                    IntVector intVector = new IntVector(name, allocator);
                    intVector.allocateNew();
                    columns.put(name, intVector);
                    break;
                case java.sql.Types.VARCHAR:
                    VarCharVector strVector = new VarCharVector(name, allocator);
                    strVector.allocateNew();
                    columns.put(name, strVector);
                    break;
                case java.sql.Types.NUMERIC, java.sql.Types.DECIMAL:
                    DecimalVector decimalVector = new DecimalVector(name, allocator, 10, 2);
                    decimalVector.allocateNew();
                    columns.put(name, decimalVector);
                    break;
                case java.sql.Types.DATE:
                    DateDayVector dateVector = new DateDayVector(name, allocator);
                    dateVector.allocateNew();
                    columns.put(name, dateVector);
                    break;
                case java.sql.Types.TIMESTAMP:
                    TimeStampMilliVector tsVector = new TimeStampMilliVector(name, allocator);
                    tsVector.allocateNew();
                    columns.put(name, tsVector);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported SQL type: " + type);
            }
        }

        int rowIndex = 0;
        while (rs.next()) {
            for (int i = 1; i <= colCount; i++) {
                FieldVector vector = columns.get(meta.getColumnLabel(i));
                if (vector instanceof IntVector) {
                    ((IntVector) vector).setSafe(rowIndex, rs.getInt(i));
                } else if (vector instanceof VarCharVector) {
                    ((VarCharVector) vector).setSafe(rowIndex, rs.getString(i).getBytes());
                } else if (vector instanceof DecimalVector) {
                    ((DecimalVector) vector).setSafe(rowIndex, rs.getBigDecimal(i));
                } else if (vector instanceof DateDayVector) {
                    ((DateDayVector) vector).setSafe(rowIndex, (int) (rs.getDate(i).getTime() / (1000 * 60 * 60 * 24)));
                } else if (vector instanceof TimeStampMilliVector) {
                    ((TimeStampMilliVector) vector).setSafe(rowIndex, rs.getTimestamp(i).getTime());
                }
            }
            rowIndex++;
        }

        final int valueCount = rowIndex;
        columns.values().forEach(vec -> vec.setValueCount(valueCount));
        return columns;
    }


}
