# Query Engine (Apache Arrow + Calcite)

Легковесный in-memory OLAP-движок, построенный с использованием **Apache Arrow** для хранения данных и **Apache Calcite** для SQL-парсинга и логического планирования.

> ⚡️ Поддерживает фильтрацию (`WHERE`) и проекцию (`SELECT col1, col2`) на собственном физическом исполнителе.

---

## 🔧 Архитектура

- **Apache Calcite**: SQL-парсинг, логическое планирование (`RelNode`)
- **Собственный Physical Planner**: преобразует `RelNode` в дерево исполняемых операторов (`PhysicalOperator`)
- **Apache Arrow**: эффективное in-memory хранение данных в колонках

---

## 📦 Примеры запросов

```sql
SELECT region, sales FROM arrow_table WHERE region = 'EU'
