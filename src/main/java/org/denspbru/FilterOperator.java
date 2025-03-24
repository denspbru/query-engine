package org.denspbru;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class FilterOperator implements PhysicalOperator {
    private final PhysicalOperator input;
    private final Predicate<Object[]> condition;

    public FilterOperator(PhysicalOperator input, Predicate<Object[]> condition) {
        this.input = input;
        this.condition = condition;
    }

    @Override
    public List<Object[]> execute() {
        return input.execute().stream()
                .filter(condition)
                .collect(Collectors.toList());
    }
}