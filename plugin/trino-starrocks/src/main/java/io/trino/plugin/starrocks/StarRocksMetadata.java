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
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class StarRocksMetadata
        implements ConnectorMetadata
{
    private final StarRocksMetadataClient metadataClient;
    private final StarRocksTypeMapper typeMapper;

    @Inject
    public StarRocksMetadata(StarRocksMetadataClient metadataClient, StarRocksTypeMapper typeMapper)
    {
        this.metadataClient = requireNonNull(metadataClient, "metadataClient is null");
        this.typeMapper = requireNonNull(typeMapper, "typeMapper is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metadataClient.listSchemaNames(session);
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        return metadataClient.getTable(session, tableName)
                .map(remoteTable -> new StarRocksTableHandle(
                        tableName.getSchemaName(),
                        tableName.getTableName(),
                        remoteTable.remoteSchemaName(),
                        remoteTable.remoteTableName(),
                        remoteTable.relationType()))
                .orElse(null);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        return metadataClient.listTables(session, optionalSchemaName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        StarRocksTableHandle tableHandle = (StarRocksTableHandle) table;
        return getRemoteTable(session, tableHandle)
                .map(this::toTableMetadata)
                .orElse(null);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        StarRocksTableHandle starRocksTableHandle = (StarRocksTableHandle) tableHandle;
        StarRocksRemoteTable remoteTable = getRemoteTable(session, starRocksTableHandle)
                .orElseThrow(() -> new TableNotFoundException(starRocksTableHandle.toSchemaTableName()));

        Map<String, ColumnHandle> columnHandles = new LinkedHashMap<>();
        for (StarRocksColumnHandle columnHandle : toColumnHandles(remoteTable.columns())) {
            ColumnHandle previous = columnHandles.putIfAbsent(columnHandle.columnName(), columnHandle);
            if (previous != null) {
                throw new TrinoException(NOT_SUPPORTED, "Duplicate column after case folding: " + columnHandle.columnName());
            }
        }
        return Collections.unmodifiableMap(columnHandles);
    }

    @SuppressWarnings("deprecation")
    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        Map<SchemaTableName, List<ColumnMetadata>> tableColumns = new LinkedHashMap<>();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            getRemoteTable(session, tableName).ifPresent(remoteTable -> tableColumns.put(tableName, toColumnMetadata(remoteTable.columns())));
        }
        return Collections.unmodifiableMap(tableColumns);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((StarRocksColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table)
    {
        StarRocksTableHandle handle = (StarRocksTableHandle) table;
        OptionalLong rowCount = metadataClient.getTableRowCount(session, handle.toSchemaTableName());
        if (rowCount.isEmpty()) {
            return TableStatistics.empty();
        }

        return TableStatistics.builder()
                .setRowCount(Estimate.of((double) rowCount.getAsLong()))
                .build();
    }

    private Optional<StarRocksRemoteTable> getRemoteTable(ConnectorSession session, StarRocksTableHandle tableHandle)
    {
        return getRemoteTable(session, tableHandle.toSchemaTableName());
    }

    private Optional<StarRocksRemoteTable> getRemoteTable(ConnectorSession session, SchemaTableName tableName)
    {
        return metadataClient.getTable(session, tableName);
    }

    private ConnectorTableMetadata toTableMetadata(StarRocksRemoteTable remoteTable)
    {
        return new ConnectorTableMetadata(remoteTable.schemaTableName(), toColumnMetadata(remoteTable.columns()));
    }

    private List<ColumnMetadata> toColumnMetadata(List<StarRocksRemoteColumn> columns)
    {
        List<ColumnMetadata> columnMetadata = new ArrayList<>(columns.size());
        for (StarRocksRemoteColumn column : columns) {
            columnMetadata.add(new ColumnMetadata(column.columnName(), typeMapper.toTrinoType(column)));
        }
        return List.copyOf(columnMetadata);
    }

    private List<StarRocksColumnHandle> toColumnHandles(List<StarRocksRemoteColumn> columns)
    {
        List<StarRocksColumnHandle> columnHandles = new ArrayList<>(columns.size());
        for (StarRocksRemoteColumn column : columns) {
            columnHandles.add(new StarRocksColumnHandle(
                    column.columnName(),
                    column.remoteColumnName(),
                    typeMapper.toTrinoType(column),
                    column.ordinalPosition() - 1));
        }
        return List.copyOf(columnHandles);
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isPresent()) {
            return List.of(prefix.toSchemaTableName());
        }
        return listTables(session, prefix.getSchema());
    }
}
