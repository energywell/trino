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

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class StarrocksAggregateFunction
{
    private final String functionName;
    private final Optional<String> columnName;
    private final boolean distinct;
    private final String outputColumnName;
    private final String starrocksOutputType;

    @JsonCreator
    public StarrocksAggregateFunction(
            @JsonProperty("functionName") String functionName,
            @JsonProperty("columnName") Optional<String> columnName,
            @JsonProperty("distinct") boolean distinct,
            @JsonProperty("outputColumnName") String outputColumnName,
            @JsonProperty("starrocksOutputType") String starrocksOutputType)
    {
        this.functionName = requireNonNull(functionName, "functionName is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.distinct = distinct;
        this.outputColumnName = requireNonNull(outputColumnName, "outputColumnName is null");
        this.starrocksOutputType = requireNonNull(starrocksOutputType, "starrocksOutputType is null");
    }

    @JsonProperty
    public String getFunctionName()
    {
        return functionName;
    }

    @JsonProperty
    public Optional<String> getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public boolean isDistinct()
    {
        return distinct;
    }

    @JsonProperty
    public String getOutputColumnName()
    {
        return outputColumnName;
    }

    @JsonProperty
    public String getStarrocksOutputType()
    {
        return starrocksOutputType;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StarrocksAggregateFunction that = (StarrocksAggregateFunction) o;
        return distinct == that.distinct &&
                Objects.equals(functionName, that.functionName) &&
                Objects.equals(columnName, that.columnName) &&
                Objects.equals(outputColumnName, that.outputColumnName) &&
                Objects.equals(starrocksOutputType, that.starrocksOutputType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionName, columnName, distinct, outputColumnName, starrocksOutputType);
    }
}
