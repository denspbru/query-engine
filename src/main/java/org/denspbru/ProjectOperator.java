package org.denspbru;

import java.util.ArrayList;
import java.util.List;

public class ProjectOperator implements PhysicalOperator {
    private final PhysicalOperator input;
    private final int[] projectionIndices;

    public ProjectOperator(PhysicalOperator input, int[] projectionIndices) {
        this.input = input;
        this.projectionIndices = projectionIndices;
    }

    @Override
    public List<Object[]> execute() {
        List<Object[]> inputRows = input.execute();
        List<Object[]> projected = new ArrayList<>();
        for (Object[] row : inputRows) {
            Object[] newRow = new Object[projectionIndices.length];
            for (int i = 0; i < projectionIndices.length; i++) {
                newRow[i] = row[projectionIndices[i]];
            }
            projected.add(newRow);
        }
        return projected;
    }
}