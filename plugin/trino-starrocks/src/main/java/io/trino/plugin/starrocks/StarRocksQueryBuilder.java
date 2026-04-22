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

import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class StarRocksQueryBuilder
{
    public String buildSelectSql(StarRocksTableHandle tableHandle, List<StarRocksColumnHandle> columns)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(columns, "columns is null");

        String projection = columns.isEmpty()
                ? "1"
                : columns.stream()
                        .map(column -> quoteIdentifier(column.remoteColumnName()) + " AS " + quoteIdentifier(column.columnName()))
                        .collect(joining(", "));

        return "SELECT " + projection +
                " FROM " + quoteIdentifier(tableHandle.remoteSchemaName()) +
                "." + quoteIdentifier(tableHandle.remoteTableName());
    }

    static String quoteIdentifier(String value)
    {
        return "`" + value.replace("`", "``") + "`";
    }
}
