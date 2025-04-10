package org.denspbru;

import org.apache.arrow.vector.FieldVector;

import java.util.*;

/**
 * Представление in-memory таблицы на основе Apache Arrow.
 */
public class Table {
    private final String name;
    private final Map<String, FieldVector> columns;

    public Table(String name, Map<String, FieldVector> columns) {
        this.name = name;
        this.columns = new LinkedHashMap<>(columns); // порядок важен для Calcite
    }

    /**
     * Имя таблицы.
     */
    public String getName() {
        return name;
    }

    /**
     * Список названий колонок в порядке их добавления.
     */
    public List<String> getColumnNames() {
        return new ArrayList<>(columns.keySet());
    }

    /**
     * Получение вектора по имени колонки.
     */
    public FieldVector getVector(String columnName) {
        return columns.get(columnName);
    }

    /**
     * Все векторы колонок (в порядке добавления).
     */
    public Collection<FieldVector> getVectors() {
        return columns.values();
    }

    /**
     * Количество строк (предполагается, что все колонки одинаковой длины).
     */
    public int getRowCount() {
        if (columns.isEmpty()) {
            return 0;
        }
        return columns.values().iterator().next().getValueCount();
    }

    /**
     * Возвращает map имя_колонки → вектор.
     */
    public Map<String, FieldVector> getColumns() {
        return Collections.unmodifiableMap(columns);
    }
}
