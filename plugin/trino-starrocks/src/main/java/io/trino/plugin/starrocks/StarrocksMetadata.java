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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.MaterializedViewNotFoundException;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class StarrocksMetadata
        implements ConnectorMetadata
{
    private final StarrocksClient client;
    private final StarrocksTypeMapper typeMapper;
    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();

    StarrocksMetadata(StarrocksClient client, StarrocksConfig config, StarrocksTypeMapper typeMapper)
    {
        this.client = client;
        this.typeMapper = typeMapper;
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return listSchemaNames(session).contains(schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return client.getFeClient().getSchemaNames(session);
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        if (!properties.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating schemas with properties");
        }
        if (schemaExists(session, schemaName)) {
            throw new TrinoException(ALREADY_EXISTS, "Schema already exists: " + schemaName);
        }
        client.getFeClient().createSchema(session, schemaName);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        if (!schemaExists(session, schemaName)) {
            throw new SchemaNotFoundException(schemaName);
        }
        if (!cascade && !listTables(session, Optional.of(schemaName)).isEmpty()) {
            throw new TrinoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + schemaName);
        }
        client.getFeClient().dropSchema(session, schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        if (!schemaExists(session, source)) {
            throw new SchemaNotFoundException(source);
        }
        if (schemaExists(session, target)) {
            throw new TrinoException(ALREADY_EXISTS, "Schema already exists: " + target);
        }
        client.getFeClient().renameSchema(session, source, target);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return client.getFeClient().listTables(schemaName, session);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        return client.getFeClient().getTableHandle(session, tableName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return client.getFeClient().listViews(schemaName, session);
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return client.getFeClient().getView(session, viewName, typeMapper);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, Map<String, Object> viewProperties, boolean replace)
    {
        if (!viewProperties.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating views with properties");
        }
        if (relationExists(session, viewName)) {
            if (!replace || getView(session, viewName).isEmpty()) {
                throw new TrinoException(ALREADY_EXISTS, "Relation already exists: " + viewName);
            }
            client.getFeClient().dropView(session, viewName);
        }
        client.getFeClient().createView(session, viewName, definition);
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        ConnectorViewDefinition definition = getView(session, source)
                .orElseThrow(() -> new ViewNotFoundException(source));
        if (relationExists(session, target)) {
            throw new TrinoException(ALREADY_EXISTS, "Relation already exists: " + target);
        }
        client.getFeClient().createView(session, target, definition);
        client.getFeClient().dropView(session, source);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        if (getView(session, viewName).isEmpty()) {
            throw new ViewNotFoundException(viewName);
        }
        client.getFeClient().dropView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return client.getFeClient().listMaterializedViews(schemaName, session);
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return client.getFeClient().getMaterializedView(session, viewName, typeMapper);
    }

    @Override
    public void createMaterializedView(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> properties,
            boolean replace,
            boolean ignoreExisting)
    {
        if (!properties.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support materialized views with properties");
        }
        if (replace) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing materialized views");
        }
        if (relationExists(session, viewName)) {
            if (ignoreExisting && getMaterializedView(session, viewName).isPresent()) {
                return;
            }
            throw new TrinoException(ALREADY_EXISTS, "Relation already exists: " + viewName);
        }
        client.getFeClient().createMaterializedView(session, viewName, definition);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        if (getMaterializedView(session, viewName).isEmpty()) {
            throw new MaterializedViewNotFoundException(viewName);
        }
        client.getFeClient().dropMaterializedView(session, viewName);
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        ConnectorMaterializedViewDefinition definition = getMaterializedView(session, source)
                .orElseThrow(() -> new MaterializedViewNotFoundException(source));
        if (relationExists(session, target)) {
            throw new TrinoException(ALREADY_EXISTS, "Relation already exists: " + target);
        }
        client.getFeClient().createMaterializedView(session, target, definition);
        client.getFeClient().dropMaterializedView(session, source);
    }

    @Override
    public Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition materializedViewDefinition)
    {
        return Map.of();
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName name)
    {
        if (getMaterializedView(session, name).isEmpty()) {
            throw new MaterializedViewNotFoundException(name);
        }
        return client.getFeClient().getMaterializedViewFreshness(name, session);
    }

    @Override
    public boolean delegateMaterializedViewRefreshToConnector(ConnectorSession session, SchemaTableName viewName)
    {
        return true;
    }

    @Override
    public CompletableFuture<?> refreshMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        if (getMaterializedView(session, viewName).isEmpty()) {
            throw new MaterializedViewNotFoundException(viewName);
        }
        client.getFeClient().refreshMaterializedView(session, viewName);
        return completedFuture(null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        StarrocksTableHandle tableHandle = (StarrocksTableHandle) table;
        return new ConnectorTableMetadata(
                tableHandle.getSchemaTableName(),
                tableHandle.getColumns().stream()
                        .map(column -> {
                            ColumnMetadata.Builder builder = ColumnMetadata.builder();
                            builder.setName(column.getColumnName());
                            builder.setType(typeMapper.toTrinoType(
                                    column.getType(),
                                    column.getColumnType(),
                                    column.getColumnSize(),
                                    column.getDecimalDigits()));
                            builder.setNullable(column.isNullable());
                            builder.setComment(Optional.ofNullable(column.getComment()));
                            builder.setExtraInfo(Optional.ofNullable(column.getExtra()));
                            return builder.build();
                        })
                        .collect(toImmutableList()),
                tableHandle.getProperties().orElse(ImmutableMap.of()),
                tableHandle.getComment());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        ((StarrocksTableHandle) tableHandle).getColumns().forEach(column -> columnHandles.put(column.getColumnName(), column));
        return columnHandles.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((StarrocksTableHandle) tableHandle).getColumns().stream()
                .filter(column -> column.getColumnName().equals(((StarrocksColumnHandle) columnHandle).getColumnName()))
                .findFirst()
                .map(column -> new ColumnMetadata(
                        column.getColumnName(),
                        typeMapper.toTrinoType(column.getType(), column.getColumnType(), column.getColumnSize(), column.getDecimalDigits())))
                .orElseThrow(() -> new IllegalArgumentException("Column not found: " + columnHandle));
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return client.getFeClient().getTableStatistics(session, (StarrocksTableHandle) tableHandle);
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        StarrocksTableHandle starrocksHandle = (StarrocksTableHandle) handle;
        Set<StarrocksColumnHandle> currentColumns = new LinkedHashSet<>(starrocksHandle.getColumns());

        ImmutableMap<String, StarrocksColumnHandle> projectedColumns = assignments.entrySet().stream()
                .filter(entry -> entry.getValue() instanceof StarrocksColumnHandle)
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> (StarrocksColumnHandle) entry.getValue()));

        if (projectedColumns.size() == currentColumns.size() &&
                new LinkedHashSet<>(projectedColumns.values()).equals(currentColumns)) {
            return Optional.empty();
        }

        StarrocksTableHandle newTableHandle = new StarrocksTableHandle(
                starrocksHandle.getSchemaTableName(),
                ImmutableList.copyOf(projectedColumns.values()),
                starrocksHandle.getConstraint(),
                starrocksHandle.getComment(),
                starrocksHandle.getPartitionKey(),
                starrocksHandle.getProperties(),
                starrocksHandle.getLimit(),
                starrocksHandle.getSortOrder());

        List<Assignment> assignmentList = projectedColumns.entrySet().stream()
                .map(entry ->
                        new Assignment(
                                entry.getKey(),
                                entry.getValue(),
                                typeMapper.toTrinoType(
                                        entry.getValue().getType(),
                                        entry.getValue().getColumnType(),
                                        entry.getValue().getColumnSize(),
                                        entry.getValue().getDecimalDigits())))
                .collect(toImmutableList());

        boolean allExpressionsHandled = projections.stream()
                .allMatch(expression -> isHandledExpression(expression, projectedColumns));

        return Optional.of(new ProjectionApplicationResult<>(
                newTableHandle,
                projections,
                assignmentList,
                allExpressionsHandled));
    }

    private boolean isHandledExpression(ConnectorExpression expression, Map<String, StarrocksColumnHandle> projectedColumns)
    {
        if (isVariableReferenceExpression(expression)) {
            return projectedColumns.containsKey(getVariableName(expression));
        }
        return false;
    }

    private boolean isVariableReferenceExpression(ConnectorExpression expression)
    {
        return expression instanceof Variable;
    }

    private String getVariableName(ConnectorExpression expression)
    {
        if (expression instanceof Variable) {
            return ((Variable) expression).getName();
        }
        throw new IllegalArgumentException("Expression is not a Variable");
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        StarrocksTableHandle handle = (StarrocksTableHandle) table;
        TupleDomain<ColumnHandle> constraintSummary = constraint.getSummary();

        if (constraintSummary.isNone()) {
            return Optional.empty();
        }

        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();

        if (oldDomain.contains(constraintSummary) && !oldDomain.isAll()) {
            return Optional.empty();
        }

        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraintSummary);

        StarrocksTableHandle newHandle = new StarrocksTableHandle(
                handle.getSchemaTableName(),
                handle.getColumns(),
                newDomain,
                handle.getComment(),
                handle.getPartitionKey(),
                handle.getProperties(),
                handle.getLimit(),
                handle.getSortOrder());

        return Optional.of(new ConstraintApplicationResult<>(newHandle, TupleDomain.all(), constraint.getExpression(), false));
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle table, long limit)
    {
        StarrocksTableHandle handle = (StarrocksTableHandle) table;

        if (handle.getLimit().isPresent() && handle.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        StarrocksTableHandle updatedHandle = new StarrocksTableHandle(
                handle.getSchemaTableName(),
                handle.getColumns(),
                handle.getConstraint(),
                handle.getComment(),
                handle.getPartitionKey(),
                handle.getProperties(),
                OptionalLong.of(limit),
                handle.getSortOrder());

        return Optional.of(new LimitApplicationResult<>(updatedHandle, true, false));
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
            ConnectorSession session,
            ConnectorTableHandle table,
            long topNCount,
            List<SortItem> sortItems,
            Map<String, ColumnHandle> assignments)
    {
        if (topNCount <= 0 || sortItems.isEmpty()) {
            return Optional.empty();
        }

        StarrocksTableHandle handle = (StarrocksTableHandle) table;

        List<SortItem> normalizedSortItems = sortItems.stream()
                .map(sortItem -> {
                    ColumnHandle assignment = assignments.get(sortItem.getName());
                    if (!(assignment instanceof StarrocksColumnHandle)) {
                        return null;
                    }
                    StarrocksColumnHandle columnHandle = (StarrocksColumnHandle) assignment;
                    return new SortItem(columnHandle.getColumnName(), sortItem.getSortOrder());
                })
                .collect(toImmutableList());

        if (normalizedSortItems.stream().anyMatch(item -> item == null)) {
            return Optional.empty();
        }

        if (handle.getSortOrder().isPresent() && !handle.getSortOrder().get().equals(normalizedSortItems)) {
            return Optional.empty();
        }

        long pushedLimit = handle.getLimit().isPresent()
                ? Math.min(handle.getLimit().getAsLong(), topNCount)
                : topNCount;

        if (handle.getSortOrder().isPresent() &&
                handle.getSortOrder().get().equals(normalizedSortItems) &&
                handle.getLimit().isPresent() &&
                handle.getLimit().getAsLong() <= pushedLimit) {
            return Optional.empty();
        }

        StarrocksTableHandle updatedHandle = new StarrocksTableHandle(
                handle.getSchemaTableName(),
                handle.getColumns(),
                handle.getConstraint(),
                handle.getComment(),
                handle.getPartitionKey(),
                handle.getProperties(),
                OptionalLong.of(pushedLimit),
                Optional.of(normalizedSortItems));

        return Optional.of(new TopNApplicationResult<>(updatedHandle, true, false));
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        if (saveMode == SaveMode.REPLACE) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }
        client.getFeClient().createTable(session, tableMetadata, saveMode, typeMapper);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(
            ConnectorSession session,
            ConnectorTableMetadata tableMetadata,
            Optional<ConnectorTableLayout> layout,
            RetryMode retryMode,
            boolean replace)
    {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }
        if (replace) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }

        String targetTableName = tableMetadata.getTable().getTableName();
        String temporaryTableName = buildTemporaryTableName(targetTableName, session.getQueryId());
        ConnectorTableMetadata temporaryTableMetadata = new ConnectorTableMetadata(
                new SchemaTableName(tableMetadata.getTable().getSchemaName(), temporaryTableName),
                tableMetadata.getColumns(),
                tableMetadata.getProperties(),
                tableMetadata.getComment(),
                tableMetadata.getCheckConstraints());
        client.getFeClient().createTable(session, temporaryTableMetadata, SaveMode.FAIL, typeMapper);
        setRollback(() -> client.getFeClient().dropTableIfExists(
                session,
                new SchemaTableName(tableMetadata.getTable().getSchemaName(), temporaryTableName)));

        List<String> columnNames = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableList());
        List<Type> columnTypes = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());

        return new StarrocksOutputTableHandle(
                tableMetadata.getTable().getSchemaName(),
                targetTableName,
                temporaryTableName,
                columnNames,
                columnTypes);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        StarrocksTableHandle starrocksTable = (StarrocksTableHandle) tableHandle;
        client.getFeClient().dropTable(session, starrocksTable.getSchemaTableName());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        StarrocksTableHandle starrocksTable = (StarrocksTableHandle) tableHandle;
        client.getFeClient().renameTable(session, starrocksTable.getSchemaTableName(), newTableName);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column, ColumnPosition position)
    {
        if (!(position instanceof ColumnPosition.Last)) {
            throw new TrinoException(NOT_SUPPORTED, "This connector only supports adding columns at the end");
        }
        StarrocksTableHandle starrocksTable = (StarrocksTableHandle) tableHandle;
        client.getFeClient().addColumn(session, starrocksTable.getSchemaTableName(), column, typeMapper);
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        StarrocksTableHandle starrocksTable = (StarrocksTableHandle) tableHandle;
        StarrocksColumnHandle columnHandle = (StarrocksColumnHandle) source;
        client.getFeClient().renameColumn(session, starrocksTable.getSchemaTableName(), columnHandle.getColumnName(), target);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        StarrocksTableHandle starrocksTable = (StarrocksTableHandle) tableHandle;
        StarrocksColumnHandle columnHandle = (StarrocksColumnHandle) column;
        client.getFeClient().dropColumn(session, starrocksTable.getSchemaTableName(), columnHandle.getColumnName());
    }

    @Override
    public void setColumnType(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Type type)
    {
        StarrocksTableHandle starrocksTable = (StarrocksTableHandle) tableHandle;
        StarrocksColumnHandle columnHandle = (StarrocksColumnHandle) column;
        client.getFeClient().setColumnType(session, starrocksTable.getSchemaTableName(), columnHandle, type, typeMapper);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns,
            RetryMode retryMode)
    {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries for inserts");
        }

        StarrocksTableHandle starrocksTable = (StarrocksTableHandle) tableHandle;
        List<StarrocksColumnHandle> columnHandles = columns.stream()
                .map(StarrocksColumnHandle.class::cast)
                .collect(toImmutableList());

        List<String> columnNames = columnHandles.stream()
                .map(StarrocksColumnHandle::getColumnName)
                .collect(toImmutableList());

        List<Type> columnTypes = columnHandles.stream()
                .map(col -> typeMapper.toTrinoType(
                        col.getType(), col.getColumnType(),
                        col.getColumnSize(), col.getDecimalDigits()))
                .collect(toImmutableList());

        return new StarrocksInsertTableHandle(
                starrocksTable.getSchemaTableName().getSchemaName(),
                starrocksTable.getSchemaTableName().getTableName(),
                columnNames,
                columnTypes);
    }

    @Override
    public Optional<ConnectorTableHandle> applyUpdate(ConnectorSession session, ConnectorTableHandle handle, Map<ColumnHandle, Constant> assignments)
    {
        StarrocksTableHandle tableHandle = (StarrocksTableHandle) handle;
        if (assignments.isEmpty()) {
            return Optional.empty();
        }

        LinkedHashMap<String, String> assignmentLiterals = new LinkedHashMap<>();
        for (Map.Entry<ColumnHandle, Constant> entry : assignments.entrySet()) {
            StarrocksColumnHandle columnHandle = (StarrocksColumnHandle) entry.getKey();
            assignmentLiterals.put(columnHandle.getColumnName(), formatLiteral(entry.getValue()));
        }
        return Optional.of(new StarrocksUpdateTableHandle(tableHandle, assignmentLiterals));
    }

    @Override
    public OptionalLong executeUpdate(ConnectorSession session, ConnectorTableHandle handle)
    {
        return client.getFeClient().executeUpdate(session, (StarrocksUpdateTableHandle) handle, client.getConfig().getTupleDomainLimit());
    }

    private String formatLiteral(Constant constant)
    {
        Type type = constant.getType();
        Object nativeValue = constant.getValue();
        Object value = type.getObjectValue(nativeValueToBlock(type, nativeValue), 0);
        if (value == null) {
            return "NULL";
        }
        if (type instanceof VarcharType || type instanceof DateType || type instanceof TimestampType) {
            return "'" + value.toString().replace("'", "''") + "'";
        }
        if (type instanceof BigintType || type instanceof IntegerType || type instanceof SmallintType || type instanceof TinyintType) {
            return value.toString();
        }
        if (type instanceof DoubleType || type instanceof RealType || type instanceof DecimalType) {
            return new BigDecimal(value.toString()).toPlainString();
        }
        if (type instanceof BooleanType) {
            return ((Boolean) value) ? "1" : "0";
        }
        if (type.getBaseName().equals(StandardTypes.JSON)) {
            String json = value.toString().replace("'", "''");
            return "parse_json('" + json + "')";
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported UPDATE literal type: " + type.getDisplayName());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(
            ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        StarrocksOutputTableHandle outputHandle = (StarrocksOutputTableHandle) tableHandle;
        client.getFeClient().renameTable(
                session,
                new SchemaTableName(outputHandle.getSchemaName(), outputHandle.getTemporaryTableName()),
                new SchemaTableName(outputHandle.getSchemaName(), outputHandle.getTableName()));
        clearRollback();
        return Optional.empty();
    }

    private boolean relationExists(ConnectorSession session, SchemaTableName relationName)
    {
        return client.getFeClient().getTableHandle(session, relationName) != null ||
                getView(session, relationName).isPresent() ||
                getMaterializedView(session, relationName).isPresent();
    }

    public void rollback()
    {
        Optional.ofNullable(rollbackAction.getAndSet(null)).ifPresent(Runnable::run);
    }

    public void clearRollback()
    {
        rollbackAction.set(null);
    }

    private void setRollback(Runnable action)
    {
        requireNonNull(action, "action is null");
        rollbackAction.set(action);
    }

    private static String buildTemporaryTableName(String targetTableName, String queryId)
    {
        String normalizedQueryId = queryId.replaceAll("[^A-Za-z0-9_]", "_");
        return targetTableName + "__trino_ctas_tmp_" + normalizedQueryId;
    }
}
