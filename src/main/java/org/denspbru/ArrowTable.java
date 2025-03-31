package org.denspbru;

import org.apache.arrow.vector.FieldVector;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.*;

public class ArrowTable implements ProjectableFilterableTable {
    private final Map<String, FieldVector> columns;
    private final List<String> columnNames;

    public ArrowTable(Map<String, FieldVector> columns) {
        this.columns = columns;
        this.columnNames = new ArrayList<>(columns.keySet());
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        Enumerator<Object[]> enumerator = new Enumerator<>() {
            private final int rowCount = columns.isEmpty() ? 0 : columns.values().iterator().next().getValueCount();
            private int currentIndex = -1;

            @Override
            public Object[] current() {
                Object[] row = new Object[projects.length];
                for (int i = 0; i < projects.length; i++) {
                    int colIndex = projects[i];
                    FieldVector vector = columns.get(columnNames.get(colIndex));
                    row[i] = vector.getObject(currentIndex);
                }
                return row;
            }

            @Override
            public boolean moveNext() {
                currentIndex++;
                return currentIndex < rowCount;
            }

            @Override
            public void reset() { currentIndex = -1; }
            @Override
            public void close() {}
        };

        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return enumerator;
            }
        };
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (String name : columnNames) {
            FieldVector vector = columns.get(name);
            SqlTypeName sqlType = mapArrowToSqlType(vector);
            builder.add(name.toUpperCase(), sqlType);
        }
        return builder.build();
    }

    private SqlTypeName mapArrowToSqlType(FieldVector vector) {
        String typeName = vector.getField().getType().getTypeID().name();
        return switch (typeName) {
            case "Int" -> SqlTypeName.INTEGER;
            case "Decimal" -> SqlTypeName.DECIMAL;
            case "Utf8" -> SqlTypeName.VARCHAR;
            case "Date" -> SqlTypeName.DATE;
            case "Timestamp" -> SqlTypeName.TIMESTAMP;
            default -> SqlTypeName.ANY;
        };
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(
            String column,
            org.apache.calcite.sql.SqlCall call,
            org.apache.calcite.sql.SqlNode parent,
            org.apache.calcite.config.CalciteConnectionConfig config) {
        return true; // разрешаем агрегации над любыми колонками
    }

    @Override
    public boolean isRolledUp(String column) {
        return false; // у нас нет свернутых (rolled-up) колонок
    }
    @Override
    public Schema.TableType getJdbcTableType() {
        return Schema.TableType.TABLE;
    }
    @Override
    public org.apache.calcite.schema.Statistic getStatistic() {
        return org.apache.calcite.schema.Statistics.UNKNOWN;
    }

}
