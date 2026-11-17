/*
 * Copyright 2026 Greptime Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.greptime.kafka.connect;

import io.greptime.models.Table;
import io.greptime.models.TableSchema;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.errors.DataException;

/**
 * Rows bound for one table. The batch schema is the union of the columns of its rows; a column missing
 * from a row is written as null.
 */
final class TableBatch {

    private final String table;
    private final LinkedHashMap<String, ColumnSpec> columns = new LinkedHashMap<>();
    private final List<Map<String, Object>> rows = new ArrayList<>();

    TableBatch(String table) {
        this.table = table;
    }

    /** Throws {@link DataException} without modifying the batch if the row disagrees with an earlier row. */
    void add(RecordConverter.Row row) {
        for (ColumnSpec column : row.columns) {
            ColumnSpec existing = this.columns.get(column.name);
            if (existing != null && !existing.equals(column)) {
                throw new DataException(String.format(
                        "Column '%s' of table '%s' is %s in this record but %s in an earlier record of the batch",
                        column.name, this.table, column, existing));
            }
        }
        Map<String, Object> values = new HashMap<>(row.columns.size() * 2);
        for (int i = 0; i < row.columns.size(); i++) {
            ColumnSpec column = row.columns.get(i);
            this.columns.putIfAbsent(column.name, column);
            values.put(column.name, row.values.get(i));
        }
        this.rows.add(values);
    }

    /** Splits the batch into tables of at most {@code maxRows} rows each. */
    List<Table> toTables(int maxRows) {
        TableSchema.Builder builder = TableSchema.newBuilder(this.table);
        for (ColumnSpec c : this.columns.values()) {
            builder.addColumn(c.name, c.semanticType, c.dataType, c.decimalExtension());
        }
        TableSchema schema = builder.build();

        List<Table> tables = new ArrayList<>((this.rows.size() + maxRows - 1) / maxRows);
        Object[] values = new Object[this.columns.size()];
        Table current = null;
        for (Map<String, Object> row : this.rows) {
            if (current == null || current.rowCount() == maxRows) {
                current = Table.from(schema);
                tables.add(current);
            }
            int i = 0;
            for (String name : this.columns.keySet()) {
                values[i++] = row.get(name);
            }
            current.addRow(values);
        }
        for (Table t : tables) {
            t.complete();
        }
        return tables;
    }
}
