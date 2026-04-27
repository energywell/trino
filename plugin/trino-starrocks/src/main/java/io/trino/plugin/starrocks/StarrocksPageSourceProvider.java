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

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class StarrocksPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final StarrocksClient client;
    private final StarrocksTypeMapper typeMapper;

    @Inject
    public StarrocksPageSourceProvider(StarrocksClient client, StarrocksTypeMapper typeMapper)
    {
        this.client = requireNonNull(client, "client is null");
        this.typeMapper = requireNonNull(typeMapper, "typeMapper is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        if (split instanceof StarrocksAggregateSplit aggregateSplit) {
            List<StarrocksColumnHandle> srColumns = columns.stream()
                    .map(StarrocksColumnHandle.class::cast)
                    .collect(toImmutableList());
            Connection connection;
            try {
                connection = client.getFeClient().openConnection(session);
            }
            catch (SQLException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to open JDBC connection for aggregate query: " + e.getMessage(), e);
            }
            try {
                return new StarrocksJdbcPageSource(connection, aggregateSplit.getAggregateQuery(), srColumns, typeMapper);
            }
            catch (RuntimeException e) {
                try {
                    connection.close();
                }
                catch (SQLException ignored) {
                }
                throw e;
            }
        }

        StarrocksSplit starrocksSplit = (StarrocksSplit) split;
        StarrocksTableHandle table = (StarrocksTableHandle) tableHandle;
        StarrocksBeReader beReader = new StarrocksBeReader(client.getConfig(), starrocksSplit.getBeNode(), columns, table.getSchemaTableName());
        beReader.openScanner(starrocksSplit.getTabletId(), starrocksSplit.getOpaquedQueryPlan());

        // Only enforce the limit per-split for unordered queries. For ORDER BY ... LIMIT N,
        // each split must return all rows so Trino's TopN operator can find the true global top N.
        OptionalLong pageSourceLimit = table.getSortOrder().isPresent() ? OptionalLong.empty() : table.getLimit();
        return new StarrocksPageSource(beReader, columns, typeMapper, pageSourceLimit);
    }
}
