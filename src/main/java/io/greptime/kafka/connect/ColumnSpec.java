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

import io.greptime.models.DataType;
import io.greptime.models.SemanticType;
import java.util.Objects;

/** Column definition derived from a record. Equality decides whether two records agree on a column. */
final class ColumnSpec {

    static final int DECIMAL_PRECISION = 38;

    final String name;
    final SemanticType semanticType;
    final DataType dataType;
    /** Only meaningful for {@link DataType#Decimal128}. */
    final int decimalScale;

    ColumnSpec(String name, SemanticType semanticType, DataType dataType) {
        this(name, semanticType, dataType, 0);
    }

    ColumnSpec(String name, SemanticType semanticType, DataType dataType, int decimalScale) {
        this.name = name;
        this.semanticType = semanticType;
        this.dataType = dataType;
        this.decimalScale = decimalScale;
    }

    DataType.DecimalTypeExtension decimalExtension() {
        return this.dataType == DataType.Decimal128
                ? new DataType.DecimalTypeExtension(DECIMAL_PRECISION, this.decimalScale)
                : null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnSpec)) {
            return false;
        }
        ColumnSpec that = (ColumnSpec) o;
        return this.decimalScale == that.decimalScale
                && this.name.equals(that.name)
                && this.semanticType == that.semanticType
                && this.dataType == that.dataType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.name, this.semanticType, this.dataType, this.decimalScale);
    }

    @Override
    public String toString() {
        String type = this.dataType == DataType.Decimal128
                ? "Decimal128(" + DECIMAL_PRECISION + "," + this.decimalScale + ")"
                : this.dataType.name();
        return this.name + " " + type + " " + this.semanticType;
    }
}
