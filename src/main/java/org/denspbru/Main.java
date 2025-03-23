package org.denspbru;

import org.apache.arrow.memory.*;
import org.apache.arrow.vector.*;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;
import org.apache.calcite.schema.*;

public class Main {
    public static void main(String[] args) throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             VarCharVector regionVector = new VarCharVector("region", allocator);
             IntVector salesVector = new IntVector("sales", allocator)) {
            // Fill vectors with sample data

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

            // Register ArrowTable with Calcite
            SchemaPlus rootSchema = Frameworks.createRootSchema(true);
            ArrowTable arrowTable = new ArrowTable(regionVector, salesVector);
            rootSchema.add("arrow_table", arrowTable);

            FrameworkConfig config = Frameworks.newConfigBuilder()
                    .defaultSchema(rootSchema)
                    .parserConfig(SqlParser.config().withCaseSensitive(false))
                    .build();

            Planner planner = Frameworks.getPlanner(config);
            SqlNode parsed = planner.parse("SELECT region, sales FROM arrow_table WHERE region = 'EU'");
            System.out.println("\n=== Parsed SQL ===");
            System.out.println(parsed.toString());

            SqlNode validated = planner.validate(parsed);
            System.out.println("\n=== Validated SQL ===");
            System.out.println(validated.toString());

            RelRoot relRoot = planner.rel(validated);
            System.out.println("\n=== Logical Plan ===");
            System.out.println(RelOptUtil.toString(relRoot.rel));

            Enumerable<Object[]> result = ((ScannableTable) arrowTable).scan(null);
            Enumerator<Object[]> enumerator = result.enumerator();

            System.out.println("\nQuery Result:");
            while (enumerator.moveNext()) {
                Object[] row = enumerator.current();
                if ("EU".equals(row[0])) {
                    System.out.println("region: " + row[0] + ", sales: " + row[1]);
                }
            }
            enumerator.close();
        }
    }
}