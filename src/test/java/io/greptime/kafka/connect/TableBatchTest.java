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
import static org.junit.jupiter.api.Assertions.assertThrows;
import io.greptime.models.DataType;
import io.greptime.models.SemanticType;
import io.greptime.models.Table;
import io.greptime.v1.RowData;
import java.util.List;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;

class TableBatchTest {

    private static final ColumnSpec TS = new ColumnSpec("ts", SemanticType.Timestamp, DataType.TimestampMillisecond);

    private static RecordConverter.Row row(long ts, ColumnSpec column, Object value) {
        RecordConverter.Row row = new RecordConverter.Row("t");
        row.add(TS, ts);
        row.add(column, value);
        return row;
    }

    @Test
    void unionsColumnsAndSplitsIntoChunks() {
        ColumnSpec a = new ColumnSpec("a", SemanticType.Field, DataType.Int64);
        ColumnSpec b = new ColumnSpec("b", SemanticType.Field, DataType.String);
        TableBatch batch = new TableBatch("t");
        batch.add(row(1, a, 1L));
        batch.add(row(2, b, "x"));
        batch.add(row(3, a, 3L));

        List<Table> tables = batch.toTables(2);
        assertEquals(2, tables.size());
        assertEquals(2, tables.get(0).rowCount());
        assertEquals(1, tables.get(1).rowCount());

        RowData.Rows first = tables.get(0).intoRowInsertRequest().getRows();
        assertEquals(3, first.getSchemaCount());
        // Row 1 has no column b, row 2 has no column a: both are written as null.
        assertEquals(
                RowData.Value.ValueDataCase.VALUEDATA_NOT_SET,
                first.getRows(0).getValues(2).getValueDataCase());
        assertEquals(
                RowData.Value.ValueDataCase.VALUEDATA_NOT_SET,
                first.getRows(1).getValues(1).getValueDataCase());
    }

    @Test
    void conflictingColumnTypeRejectsOnlyThatRow() {
        TableBatch batch = new TableBatch("t");
        batch.add(row(1, new ColumnSpec("v", SemanticType.Field, DataType.Int64), 1L));
        assertThrows(
                DataException.class,
                () -> batch.add(row(2, new ColumnSpec("v", SemanticType.Field, DataType.Float64), 1.5)));

        List<Table> tables = batch.toTables(10);
        assertEquals(1, tables.get(0).rowCount());
        assertEquals(2, tables.get(0).intoRowInsertRequest().getRows().getSchemaCount());
    }
}
