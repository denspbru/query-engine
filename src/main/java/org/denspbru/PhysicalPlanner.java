package org.denspbru;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.function.Predicate;

public class PhysicalPlanner {
    public PhysicalOperator build(RelNode rel) {
        if (rel instanceof Filter filter) {
            PhysicalOperator input = build(filter.getInput());
            RexNode condition = filter.getCondition();
            // Временно хардкодим предикат для region = 'EU'
            Predicate<Object[]> predicate = row -> "EU".equals(row[0]);
            return new FilterOperator(input, predicate);

        } else if (rel instanceof Project project) {
            PhysicalOperator input = build(project.getInput());
            int[] indices = project.getProjects().stream()
                    .mapToInt(expr -> ((RexInputRef) expr).getIndex())
                    .toArray();
            return new ProjectOperator(input, indices);

        } else if (rel instanceof TableScan scan) {
            RelOptTable table = scan.getTable();
            return new TableScanOperator(table);
        }

        throw new UnsupportedOperationException("Unsupported node: " + rel.getClass());
    }
}