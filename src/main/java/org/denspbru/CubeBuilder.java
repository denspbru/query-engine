package org.denspbru;

import org.apache.calcite.DataContext;
import java.util.*;

public class CubeBuilder {

    public static class Agg {
        long sum = 0;
        int count = 0;

        public void add(long val) {
            sum += val;
            count++;
        }

        public double avg() {
            return count == 0 ? 0 : (double) sum / count;
        }

        @Override
        public String toString() {
            return "Agg{sum=" + sum + ", count=" + count + ", avg=" + avg() + '}';
        }
    }

    private static int[] identityProjects(int count) {
        int[] projects = new int[count];
        for (int i = 0; i < count; i++) {
            projects[i] = i;
        }
        return projects;
    }

    public static Map<List<Object>, Agg> buildCube(
            ArrowTable table,
            List<String> dimensions,
            String measureColumn
    ) {
        Map<List<Object>, Agg> cube = new HashMap<>();
        List<String> columnNames = table.getColumnNames();
        int[] dimIndexes = dimensions.stream()
                .map(String::toUpperCase)
                .mapToInt(columnNames::indexOf)
                .toArray();
        int measureIndex = columnNames.indexOf(measureColumn.toUpperCase());

        for (Object[] row : table.scan(null, List.of(), identityProjects(table.getColumnNames().size()))) {
            List<Object> key = new ArrayList<>();
            for (int dimIndex : dimIndexes) {
                key.add(row[dimIndex]);
            }

            Object val = row[measureIndex];
            if (val != null && val instanceof Number num) {
                cube.computeIfAbsent(key, k -> new Agg()).add(num.longValue());
            }
        }

        return cube;
    }
}
