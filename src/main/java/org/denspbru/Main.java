package org.denspbru;

import org.apache.arrow.memory.*;
import org.apache.arrow.vector.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;
import org.apache.calcite.schema.*;

import java.util.*;

public class Main {
    public static void main(String[] args) throws Exception {
        try (BufferAllocator allocator = new RootAllocator()) {
            try (VarCharVector regionVector = new VarCharVector("region", allocator);
                 IntVector salesVector = new IntVector("sales", allocator)) {

                regionVector.allocateNew(4);
                salesVector.allocateNew(4);

                regionVector.setSafe(0, "EU".getBytes());
                regionVector.setSafe(1, "US".getBytes());
                regionVector.setSafe(2, "EU".getBytes());
                regionVector.setSafe(3, "ASIA".getBytes());

                salesVector.set(0, 100);
                salesVector.set(1, 200);
                salesVector.set(2, 150);
                salesVector.set(3, 300);

                regionVector.setValueCount(4);
                salesVector.setValueCount(4);

                SchemaPlus rootSchema = Frameworks.createRootSchema(true);
                ArrowTable arrowTable = new ArrowTable(regionVector, salesVector);
                rootSchema.add("arrow_table", arrowTable);

                FrameworkConfig config = Frameworks.newConfigBuilder()
                        .defaultSchema(rootSchema)
                        .parserConfig(SqlParser.config().withCaseSensitive(false))
                        .build();

                Planner planner = Frameworks.getPlanner(config);
                SqlNode parsed = planner.parse("SELECT region, sales FROM arrow_table WHERE region = 'EU'");
                SqlNode validated = planner.validate(parsed);
                RelRoot relRoot = planner.rel(validated);

                System.out.println("\n=== Logical Plan ===");
                System.out.println(RelOptUtil.toString(relRoot.rel));

                PhysicalPlanner physicalPlanner = new PhysicalPlanner();
                PhysicalOperator physicalPlan = physicalPlanner.build(relRoot.rel);

                List<Object[]> result = physicalPlan.execute();
                System.out.println("\n=== Executed Result ===");
                result.forEach(row -> System.out.println(Arrays.toString(row)));
            }
        }
    }
}