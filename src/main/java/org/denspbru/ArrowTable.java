package org.denspbru;

import org.apache.arrow.vector.FieldVector;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.*;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.config.CalciteConnectionConfig;

import java.lang.reflect.Type;
import java.util.*;

public class ArrowTable implements ScannableTable {
    private final Map<String, FieldVector> columns;
    private final List<String> columnNames;

    public ArrowTable(Map<String, FieldVector> columns) {
        this.columns = columns;
        this.columnNames = new ArrayList<>(columns.keySet());
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (String name : columnNames) {
            FieldVector vec = columns.get(name);
            SqlTypeName type;
            switch (vec.getMinorType()) {
                case INT: type = SqlTypeName.INTEGER; break;
                case FLOAT8: type = SqlTypeName.DOUBLE; break;
                case DECIMAL: type = SqlTypeName.DECIMAL; break;
                case VARCHAR: type = SqlTypeName.VARCHAR; break;
                case DATEDAY: type = SqlTypeName.DATE; break;
                case TIMESTAMPMILLI: type = SqlTypeName.TIMESTAMP; break;
                default: type = SqlTypeName.ANY;
            }
            builder.add(name, type);
        }
        return builder.build();
    }

    @Override
    public Statistic getStatistic() {
        return Statistics.UNKNOWN;
    }

    @Override
    public boolean isRolledUp(String column) {
        return false;
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
        return true;
    }

    @Override
    public Schema.TableType getJdbcTableType() {
        return Schema.TableType.TABLE;
    }
}
