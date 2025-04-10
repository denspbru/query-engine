package org.denspbru;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Хранилище таблиц в оперативной памяти.
 * Используется для SQL-запросов и работы с Apache Arrow.
 */
public class InMemoryStore {

    // Singleton instance
    public static final InMemoryStore INSTANCE = new InMemoryStore();

    // Таблицы по имени
    private final Map<String, Table> tables = new HashMap<>();

    // Приватный конструктор
    private InMemoryStore() {}

    /**
     * Регистрация новой таблицы.
     * @param name имя таблицы
     * @param table объект таблицы
     */
    public void register(String name, Table table) {
        tables.put(name, table);
    }

    /**
     * Получение таблицы по имени.
     * @param name имя таблицы
     * @return таблица или null
     */
    public Table getTable(String name) {
        return tables.get(name);
    }

    /**
     * Все зарегистрированные таблицы (нельзя изменять).
     * @return имя → таблица
     */
    public Map<String, Table> getTables() {
        return Collections.unmodifiableMap(tables);
    }

    /**
     * Очистка всех таблиц (например, при перезагрузке).
     */
    public void clear() {
        tables.clear();
    }
}
