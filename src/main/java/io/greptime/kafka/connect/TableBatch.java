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
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Rows bound for one table, each with the record it came from. All rows agree on the type of a shared
 * column; a column missing from a row is written as null.
 */
final class TableBatch {

    private final String table;
    private final LinkedHashMap<String, ColumnSpec> columns = new LinkedHashMap<>();
    private final List<Map<String, Object>> rows = new ArrayList<>();
    private final List<SinkRecord> records = new ArrayList<>();

    TableBatch(String table) {
        this.table = table;
    }

    /** Throws {@link DataException} without modifying the batch if the row disagrees with an earlier row. */
    void add(RecordConverter.Row row, SinkRecord record) {
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
        this.records.add(record);
    }

    int size() {
        return this.rows.size();
    }

    SinkRecord record(int index) {
        return this.records.get(index);
    }

    /**
     * Builds a table with rows {@code [from, to)}. The schema only has the columns present in those rows,
     * so a single row isolated from a rejected request does not carry columns of the rows that caused it.
     */
    Table toTable(int from, int to) {
        List<Map<String, Object>> range = this.rows.subList(from, to);
        List<ColumnSpec> present = new ArrayList<>(this.columns.size());
        for (ColumnSpec c : this.columns.values()) {
            for (Map<String, Object> row : range) {
                if (row.containsKey(c.name)) {
                    present.add(c);
                    break;
                }
            }
        }
        TableSchema.Builder builder = TableSchema.newBuilder(this.table);
        for (ColumnSpec c : present) {
            builder.addColumn(c.name, c.semanticType, c.dataType, c.decimalExtension());
        }
        Table table = Table.from(builder.build());
        Object[] values = new Object[present.size()];
        for (Map<String, Object> row : range) {
            for (int i = 0; i < values.length; i++) {
                values[i] = row.get(present.get(i).name);
            }
            table.addRow(values);
        }
        return table.complete();
    }
}
