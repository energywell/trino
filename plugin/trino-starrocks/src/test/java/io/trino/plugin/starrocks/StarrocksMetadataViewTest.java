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

import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.predicate.TupleDomain;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

public class StarrocksMetadataViewTest
{
    private static final StarrocksConfig CONFIG = new StarrocksConfig()
            .setJdbcURL("jdbc:mysql://localhost:9030")
            .setScanURL("localhost:8030")
            .setUsername("starrocks")
            .setPassword("");

    @Test
    public void testViewAndMaterializedViewMetadataRouting()
    {
        FakeStarrocksFEClient feClient = new FakeStarrocksFEClient();
        StarrocksMetadata metadata = newMetadata(feClient);

        SchemaTableName tableName = new SchemaTableName("test", "orders");
        StarrocksTableHandle tableHandle = new StarrocksTableHandle(
                tableName,
                List.of(new StarrocksColumnHandle("orderkey", 1, "bigint", "bigint", false, "", "", 0, 0)),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty());
        feClient.tables.put(tableName, tableHandle);

        SchemaTableName viewName = new SchemaTableName("test", "orders_view");
        ConnectorViewDefinition viewDefinition = new ConnectorViewDefinition(
                "SELECT orderkey FROM test.orders",
                Optional.of("starrocks"),
                Optional.of("test"),
                List.of(new ConnectorViewDefinition.ViewColumn("orderkey", BIGINT.getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.of("starrocks"),
                false,
                List.of());
        feClient.views.put(viewName, viewDefinition);

        SchemaTableName materializedViewName = new SchemaTableName("test", "orders_mv");
        ConnectorMaterializedViewDefinition materializedViewDefinition = new ConnectorMaterializedViewDefinition(
                "SELECT orderkey FROM test.orders",
                Optional.empty(),
                Optional.of("starrocks"),
                Optional.of("test"),
                List.of(new ConnectorMaterializedViewDefinition.Column("orderkey", BIGINT.getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of("starrocks"),
                List.of());
        feClient.materializedViews.put(materializedViewName, materializedViewDefinition);
        feClient.materializedViewFreshness = new MaterializedViewFreshness(
                MaterializedViewFreshness.Freshness.FRESH,
                Optional.of(Instant.parse("2026-03-05T00:00:00Z")));

        assertThat(metadata.listViews(SESSION, Optional.of("test"))).containsExactly(viewName);
        assertThat(metadata.getView(SESSION, viewName)).contains(viewDefinition);

        assertThat(metadata.listMaterializedViews(SESSION, Optional.of("test"))).containsExactly(materializedViewName);
        assertThat(metadata.getMaterializedView(SESSION, materializedViewName)).contains(materializedViewDefinition);
        assertThat(metadata.getMaterializedViewFreshness(SESSION, materializedViewName)).isEqualTo(feClient.materializedViewFreshness);

        assertThat(metadata.getTableHandle(SESSION, tableName, Optional.empty(), Optional.empty())).isEqualTo(tableHandle);
        assertThat(metadata.getTableHandle(SESSION, viewName, Optional.empty(), Optional.empty())).isNull();
        assertThat(metadata.getTableHandle(SESSION, materializedViewName, Optional.empty(), Optional.empty())).isNull();

        metadata.refreshMaterializedView(SESSION, materializedViewName);
        assertThat(feClient.refreshedMaterializedViews).containsExactly(materializedViewName);
    }

    @Test
    public void testRenameViewAndMaterializedView()
    {
        FakeStarrocksFEClient feClient = new FakeStarrocksFEClient();
        StarrocksMetadata metadata = newMetadata(feClient);

        SchemaTableName sourceView = new SchemaTableName("test", "source_view");
        feClient.views.put(sourceView, new ConnectorViewDefinition(
                "SELECT 1 AS c",
                Optional.of("starrocks"),
                Optional.of("test"),
                List.of(new ConnectorViewDefinition.ViewColumn("c", BIGINT.getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.of("starrocks"),
                false,
                List.of()));

        SchemaTableName sourceMaterializedView = new SchemaTableName("test", "source_mv");
        feClient.materializedViews.put(sourceMaterializedView, new ConnectorMaterializedViewDefinition(
                "SELECT 1 AS c",
                Optional.empty(),
                Optional.of("starrocks"),
                Optional.of("test"),
                List.of(new ConnectorMaterializedViewDefinition.Column("c", BIGINT.getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of("starrocks"),
                List.of()));

        SchemaTableName targetView = new SchemaTableName("test", "target_view");
        metadata.renameView(SESSION, sourceView, targetView);
        assertThat(feClient.views).containsKey(targetView).doesNotContainKey(sourceView);

        SchemaTableName targetMaterializedView = new SchemaTableName("test", "target_mv");
        metadata.renameMaterializedView(SESSION, sourceMaterializedView, targetMaterializedView);
        assertThat(feClient.materializedViews).containsKey(targetMaterializedView).doesNotContainKey(sourceMaterializedView);
    }

    @Test
    public void testGetTableMetadataHandlesNullCommentAndExtra()
    {
        StarrocksMetadata metadata = newMetadata(new FakeStarrocksFEClient());
        StarrocksTableHandle tableHandle = new StarrocksTableHandle(
                new SchemaTableName("test", "orders"),
                List.of(new StarrocksColumnHandle("orderkey", 1, "bigint", "bigint", false, null, null, 0, 0)),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty());

        assertThatCode(() -> metadata.getTableMetadata(SESSION, tableHandle))
                .doesNotThrowAnyException();
    }

    @Test
    public void testApplyLimitPushdown()
    {
        StarrocksMetadata metadata = newMetadata(new FakeStarrocksFEClient());
        StarrocksTableHandle tableHandle = new StarrocksTableHandle(
                new SchemaTableName("test", "orders"),
                List.of(new StarrocksColumnHandle("orderkey", 1, "bigint", "bigint", false, "", "", 0, 0)),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty());

        StarrocksTableHandle pushedHandle = (StarrocksTableHandle) metadata.applyLimit(SESSION, tableHandle, 100)
                .orElseThrow()
                .getHandle();

        assertThat(pushedHandle.getLimit()).hasValue(100);
    }

    @Test
    public void testApplyTopNPushdown()
    {
        StarrocksMetadata metadata = newMetadata(new FakeStarrocksFEClient());
        StarrocksColumnHandle column = new StarrocksColumnHandle("orderkey", 1, "bigint", "bigint", false, "", "", 0, 0);
        StarrocksTableHandle tableHandle = new StarrocksTableHandle(
                new SchemaTableName("test", "orders"),
                List.of(column),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty());

        StarrocksTableHandle pushedHandle = (StarrocksTableHandle) metadata.applyTopN(
                SESSION,
                tableHandle,
                10,
                List.of(new SortItem("c1", SortOrder.DESC_NULLS_LAST)),
                Map.of("c1", column))
                .orElseThrow()
                .getHandle();

        assertThat(pushedHandle.getLimit()).hasValue(10);
        assertThat(pushedHandle.getSortOrder()).isPresent();
        assertThat(pushedHandle.getSortOrder().orElseThrow())
                .containsExactly(new SortItem("orderkey", SortOrder.DESC_NULLS_LAST));
    }

    private static StarrocksMetadata newMetadata(FakeStarrocksFEClient feClient)
    {
        StarrocksClient client = new StarrocksClient(CONFIG, feClient, new StarrocksBEClient(CONFIG));
        return new StarrocksMetadata(client, CONFIG, new StarrocksTypeMapper(TESTING_TYPE_MANAGER));
    }

    private static class FakeStarrocksFEClient
            extends StarrocksFEClient
    {
        private final Map<SchemaTableName, StarrocksTableHandle> tables = new HashMap<>();
        private final Map<SchemaTableName, ConnectorViewDefinition> views = new HashMap<>();
        private final Map<SchemaTableName, ConnectorMaterializedViewDefinition> materializedViews = new HashMap<>();
        private MaterializedViewFreshness materializedViewFreshness = new MaterializedViewFreshness(MaterializedViewFreshness.Freshness.UNKNOWN, Optional.empty());
        private final List<SchemaTableName> refreshedMaterializedViews = new ArrayList<>();

        private FakeStarrocksFEClient()
        {
            super(CONFIG, "starrocks", null, null);
        }

        @Override
        public List<SchemaTableName> listTables(Optional<String> schemaName, ConnectorSession session)
        {
            List<SchemaTableName> relations = new ArrayList<>();
            relations.addAll(filterSchema(tables.keySet().stream().toList(), schemaName));
            relations.addAll(filterSchema(views.keySet().stream().toList(), schemaName));
            relations.addAll(filterSchema(materializedViews.keySet().stream().toList(), schemaName));
            return relations;
        }

        @Override
        public List<SchemaTableName> listViews(Optional<String> schemaName, ConnectorSession session)
        {
            return filterSchema(views.keySet().stream().toList(), schemaName);
        }

        @Override
        public List<SchemaTableName> listMaterializedViews(Optional<String> schemaName, ConnectorSession session)
        {
            return filterSchema(materializedViews.keySet().stream().toList(), schemaName);
        }

        @Override
        public StarrocksTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
        {
            return tables.get(tableName);
        }

        @Override
        public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName, StarrocksTypeMapper typeMapper)
        {
            return Optional.ofNullable(views.get(viewName));
        }

        @Override
        public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName, StarrocksTypeMapper typeMapper)
        {
            return Optional.ofNullable(materializedViews.get(viewName));
        }

        @Override
        public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition)
        {
            views.put(viewName, definition);
        }

        @Override
        public void dropView(ConnectorSession session, SchemaTableName viewName)
        {
            views.remove(viewName);
        }

        @Override
        public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition)
        {
            materializedViews.put(viewName, definition);
        }

        @Override
        public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
        {
            materializedViews.remove(viewName);
        }

        @Override
        public void refreshMaterializedView(ConnectorSession session, SchemaTableName viewName)
        {
            refreshedMaterializedViews.add(viewName);
        }

        @Override
        public MaterializedViewFreshness getMaterializedViewFreshness(SchemaTableName name, ConnectorSession session)
        {
            return materializedViewFreshness;
        }

        private static List<SchemaTableName> filterSchema(List<SchemaTableName> names, Optional<String> schemaName)
        {
            if (schemaName.isEmpty()) {
                return names;
            }
            return names.stream()
                    .filter(name -> schemaName.get().equals(name.getSchemaName()))
                    .toList();
        }
    }
}
