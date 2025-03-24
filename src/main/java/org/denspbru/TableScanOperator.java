package org.denspbru;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.schema.ScannableTable;

import java.util.List;

public class TableScanOperator implements PhysicalOperator {
    private final RelOptTable table;

    public TableScanOperator(RelOptTable table) {
        this.table = table;
    }

    @Override
    public List<Object[]> execute() {
        ScannableTable scannable = table.unwrap(ScannableTable.class);
        Enumerable<Object[]> enumerable = scannable.scan(null);
        return enumerable.toList();
    }
}