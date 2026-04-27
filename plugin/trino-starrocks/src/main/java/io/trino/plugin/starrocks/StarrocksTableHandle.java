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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortItem;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

public class StarrocksTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName schemaTableName;
    private final List<StarrocksColumnHandle> columns;
    private final TupleDomain<ColumnHandle> constraint;
    private final Optional<String> comment;
    private final Optional<List<String>> partitionKey;
    private final Optional<Map<String, Object>> properties;
    private final OptionalLong limit;
    private final Optional<List<SortItem>> sortOrder;
    private final Optional<List<StarrocksAggregateFunction>> pushdownAggregates;
    private final Optional<List<String>> groupingColumns;
    private final Optional<String> havingSql;
    private final Optional<String> expressionFilter;
    private final Optional<String> joinSqlBase;

    @JsonCreator
    public StarrocksTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("columns") List<StarrocksColumnHandle> columns,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("comment") Optional<String> comment,
            @JsonProperty("partitionKey") Optional<List<String>> partitionKey,
            @JsonProperty("properties") Optional<Map<String, Object>> properties,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("sortOrder") Optional<List<SortItem>> sortOrder,
            @JsonProperty("pushdownAggregates") Optional<List<StarrocksAggregateFunction>> pushdownAggregates,
            @JsonProperty("groupingColumns") Optional<List<String>> groupingColumns,
            @JsonProperty("havingSql") Optional<String> havingSql,
            @JsonProperty("expressionFilter") Optional<String> expressionFilter,
            @JsonProperty("joinSqlBase") Optional<String> joinSqlBase)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.comment = comment;
        this.partitionKey = partitionKey;
        this.properties = properties;
        this.limit = requireNonNull(limit, "limit is null");
        this.sortOrder = requireNonNull(sortOrder, "sortOrder is null");
        this.pushdownAggregates = requireNonNullElse(pushdownAggregates, Optional.empty());
        this.groupingColumns = requireNonNullElse(groupingColumns, Optional.empty());
        this.havingSql = requireNonNullElse(havingSql, Optional.empty());
        this.expressionFilter = requireNonNullElse(expressionFilter, Optional.empty());
        this.joinSqlBase = requireNonNullElse(joinSqlBase, Optional.empty());
    }

    public StarrocksTableHandle(
            SchemaTableName schemaTableName,
            List<StarrocksColumnHandle> columns,
            TupleDomain<ColumnHandle> constraint,
            Optional<String> comment,
            Optional<List<String>> partitionKey,
            Optional<Map<String, Object>> properties,
            OptionalLong limit,
            Optional<List<SortItem>> sortOrder,
            Optional<List<StarrocksAggregateFunction>> pushdownAggregates,
            Optional<List<String>> groupingColumns)
    {
        this(schemaTableName, columns, constraint, comment, partitionKey, properties, limit, sortOrder,
                pushdownAggregates, groupingColumns, Optional.empty(), Optional.empty(), Optional.empty());
    }

    public StarrocksTableHandle(
            SchemaTableName schemaTableName,
            List<StarrocksColumnHandle> columns,
            TupleDomain<ColumnHandle> constraint,
            Optional<String> comment,
            Optional<List<String>> partitionKey,
            Optional<Map<String, Object>> properties,
            OptionalLong limit,
            Optional<List<SortItem>> sortOrder)
    {
        this(schemaTableName, columns, constraint, comment, partitionKey, properties, limit, sortOrder,
                Optional.empty(), Optional.empty());
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public List<StarrocksColumnHandle> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    @JsonProperty
    public Optional<List<String>> getPartitionKey()
    {
        return partitionKey;
    }

    @JsonProperty
    public Optional<Map<String, Object>> getProperties()
    {
        return properties;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    @JsonProperty
    public Optional<List<SortItem>> getSortOrder()
    {
        return sortOrder;
    }

    @JsonProperty
    public Optional<List<StarrocksAggregateFunction>> getPushdownAggregates()
    {
        return pushdownAggregates;
    }

    @JsonProperty
    public Optional<List<String>> getGroupingColumns()
    {
        return groupingColumns;
    }

    @JsonProperty
    public Optional<String> getHavingSql()
    {
        return havingSql;
    }

    @JsonProperty
    public Optional<String> getExpressionFilter()
    {
        return expressionFilter;
    }

    @JsonProperty
    public Optional<String> getJoinSqlBase()
    {
        return joinSqlBase;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, columns, constraint, limit, sortOrder,
                pushdownAggregates, groupingColumns, havingSql, expressionFilter, joinSqlBase);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        StarrocksTableHandle other = (StarrocksTableHandle) obj;
        return Objects.equals(this.schemaTableName, other.schemaTableName) &&
                Objects.equals(this.columns, other.columns) &&
                Objects.equals(this.constraint, other.constraint) &&
                Objects.equals(this.limit, other.limit) &&
                Objects.equals(this.sortOrder, other.sortOrder) &&
                Objects.equals(this.pushdownAggregates, other.pushdownAggregates) &&
                Objects.equals(this.groupingColumns, other.groupingColumns) &&
                Objects.equals(this.havingSql, other.havingSql) &&
                Objects.equals(this.expressionFilter, other.expressionFilter) &&
                Objects.equals(this.joinSqlBase, other.joinSqlBase);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(schemaTableName);
        if (constraint.isNone()) {
            builder.append(" constraint=FALSE");
        }
        else if (!constraint.isAll()) {
            builder.append(" constraint on ");
            builder.append(constraint.getDomains().orElseThrow().keySet().stream()
                    .map(columnHandle -> ((StarrocksColumnHandle) columnHandle).getColumnName())
                    .collect(Collectors.joining(", ", "[", "]")));
        }
        if (!constraint.isNone()) {
            builder.append(" constraints=").append(constraint);
        }
        if (!columns.isEmpty()) {
            builder.append(" columns=").append(columns);
        }
        if (limit.isPresent()) {
            builder.append(" limit=").append(limit.getAsLong());
        }
        sortOrder.ifPresent(order -> builder.append(" sortOrder=").append(order));
        pushdownAggregates.ifPresent(aggs -> builder.append(" aggregates=").append(aggs));
        joinSqlBase.ifPresent(j -> builder.append(" join=true"));
        return builder.toString();
    }
}
