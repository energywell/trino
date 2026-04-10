/*
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
package io.trino.plugin.starrocks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

/**
 * A serializable assignment (column name + Trino type + value) carried by
 * {@link StarrocksUpdateTableHandle}.  The value is stored as a plain string
 * so that it can be round-tripped through Trino's JSON handle serialization
 * without type-erasure issues.  A null {@code serializedValue} represents
 * SQL NULL.
 */
public class StarrocksAssignment
{
    private final String columnName;
    private final String trinoTypeName;
    private final String serializedValue; // null means SQL NULL

    @JsonCreator
    public StarrocksAssignment(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("trinoTypeName") String trinoTypeName,
            @JsonProperty("serializedValue") String serializedValue)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.trinoTypeName = requireNonNull(trinoTypeName, "trinoTypeName is null");
        this.serializedValue = serializedValue;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public String getTrinoTypeName()
    {
        return trinoTypeName;
    }

    @JsonProperty
    public String getSerializedValue()
    {
        return serializedValue;
    }
}
