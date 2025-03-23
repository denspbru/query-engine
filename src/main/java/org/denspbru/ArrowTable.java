package org.denspbru;

import org.apache.arrow.vector.*;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.schema.*;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

public class ArrowTable implements ScannableTable {
    private final VarCharVector regionVector;
    private final IntVector salesVector;

    public ArrowTable(VarCharVector regionVector, IntVector salesVector) {
        this.regionVector = regionVector;
        this.salesVector = salesVector;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new Enumerator<>() {
                    int index = -1;

                    @Override
                    public Object[] current() {
                        String region = new String(regionVector.get(index));
                        int sales = salesVector.get(index);
                        return new Object[]{region, sales};
                    }

                    @Override
                    public boolean moveNext() {
                        index++;
                        return index < regionVector.getValueCount();
                    }

                    @Override
                    public void reset() {
                        index = -1;
                    }

                    @Override
                    public void close() {}
                };
            }
        };
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
                .add("region", SqlTypeName.VARCHAR)
                .add("sales", SqlTypeName.INTEGER)
                .build();
    }

    @Override
    public Schema.TableType getJdbcTableType() {
        return Schema.TableType.TABLE;
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
}