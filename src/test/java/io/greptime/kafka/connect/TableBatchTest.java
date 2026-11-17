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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.greptime.models.DataType;
import io.greptime.models.SemanticType;
import io.greptime.models.Table;
import io.greptime.v1.RowData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class TableBatchTest {

    private static final ColumnSpec TS = new ColumnSpec("ts", SemanticType.Timestamp, DataType.TimestampMillisecond);
    private static final ColumnSpec A = new ColumnSpec("a", SemanticType.Field, DataType.Int64);
    private static final ColumnSpec B = new ColumnSpec("b", SemanticType.Field, DataType.String);

    private static RecordConverter.Row row(long ts, ColumnSpec column, Object value) {
        RecordConverter.Row row = new RecordConverter.Row("t");
        row.add(TS, ts);
        row.add(column, value);
        return row;
    }

    private static List<String> columnNames(Table table) {
        List<String> names = new ArrayList<>();
        for (RowData.ColumnSchema c : table.intoRowInsertRequest().getRows().getSchemaList()) {
            names.add(c.getColumnName());
        }
        return names;
    }

    @Test
    void rangeSchemaHasOnlyColumnsOfItsRows() {
        TableBatch batch = new TableBatch("t");
        batch.add(row(1, A, 1L), null);
        batch.add(row(2, B, "x"), null);
        batch.add(row(3, A, 3L), null);

        Table all = batch.toTable(0, 3);
        assertEquals(Arrays.asList("ts", "a", "b"), columnNames(all));
        // Row 1 has no column b, row 2 has no column a: both are written as null.
        RowData.Rows rows = all.intoRowInsertRequest().getRows();
        assertEquals(
                RowData.Value.ValueDataCase.VALUEDATA_NOT_SET,
                rows.getRows(0).getValues(2).getValueDataCase());
        assertEquals(
                RowData.Value.ValueDataCase.VALUEDATA_NOT_SET,
                rows.getRows(1).getValues(1).getValueDataCase());

        // A row isolated from a rejected request must not carry columns that only other rows have.
        assertEquals(Arrays.asList("ts", "a"), columnNames(batch.toTable(2, 3)));
        assertEquals(Arrays.asList("ts", "b"), columnNames(batch.toTable(1, 2)));
    }

    @Test
    void rowWithConflictingColumnTypeIsNotAccepted() {
        TableBatch batch = new TableBatch("t");
        batch.add(row(1, A, 1L), null);
        assertTrue(batch.accepts(row(2, B, "x")));
        assertFalse(batch.accepts(row(3, new ColumnSpec("a", SemanticType.Field, DataType.Float64), 1.5)));
    }
}
