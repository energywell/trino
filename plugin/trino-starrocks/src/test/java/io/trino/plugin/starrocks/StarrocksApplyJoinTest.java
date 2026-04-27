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

import io.trino.spi.connector.BasicRelationStatistics;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class StarrocksApplyJoinTest
{
    private static final StarrocksConfig CONFIG = new StarrocksConfig()
            .setJdbcURL("jdbc:mysql://localhost:9030")
            .setScanURL("localhost:8030")
            .setUsername("starrocks")
            .setPassword("");

    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new StarrocksSessionProperties(CONFIG).getSessionProperties())
            .build();

    private static final SchemaTableName LEFT_TABLE = new SchemaTableName("test", "orders");
    private static final SchemaTableName RIGHT_TABLE = new SchemaTableName("test", "items");

    private static final StarrocksColumnHandle L_ID = col("id", 0, "bigint");
    private static final StarrocksColumnHandle L_NAME = col("name", 1, "varchar");
    private static final StarrocksColumnHandle R_ID = col("id", 0, "bigint");
    private static final StarrocksColumnHandle R_DESC = col("description", 1, "varchar");

    @Test
    public void testInnerJoinProducesSubquerySqlShape()
    {
        String sql = callApplyJoin(JoinType.INNER, leftHandle(), rightHandle());

        assertThat(sql).contains("FROM (SELECT * FROM `test`.`orders`) AS _l");
        assertThat(sql).contains("INNER JOIN (SELECT * FROM `test`.`items`) AS _r");
        assertNoOuterWhere(sql);
    }

    @Test
    public void testLeftOuterJoinHasNoOuterWhere()
    {
        String sql = callApplyJoin(JoinType.LEFT_OUTER, leftHandle(), rightHandle());

        assertThat(sql).contains("LEFT OUTER JOIN");
        assertNoOuterWhere(sql);
    }

    @Test
    public void testRightOuterJoinHasNoOuterWhere()
    {
        String sql = callApplyJoin(JoinType.RIGHT_OUTER, leftHandle(), rightHandle());

        assertThat(sql).contains("RIGHT OUTER JOIN");
        assertNoOuterWhere(sql);
    }

    @Test
    public void testFullOuterJoinHasNoOuterWhere()
    {
        String sql = callApplyJoin(JoinType.FULL_OUTER, leftHandle(), rightHandle());

        assertThat(sql).contains("FULL OUTER JOIN");
        assertNoOuterWhere(sql);
    }

    @Test
    public void testLeftSidePredicateGoesInsideLeftSubquery()
    {
        StarrocksTableHandle leftWithFilter = handleWithExpressionFilter(
                LEFT_TABLE, List.of(L_ID, L_NAME), "(`status` = 'active')");

        String sql = callApplyJoin(JoinType.LEFT_OUTER, leftWithFilter, rightHandle());

        // Predicate is inside the left subquery (before ") AS _l")
        int leftSubqueryClose = sql.indexOf(") AS _l");
        assertThat(sql.substring(0, leftSubqueryClose)).contains("WHERE");

        // Right subquery has no predicate
        int rightSubqueryStart = leftSubqueryClose + 7;
        int rightSubqueryClose = sql.indexOf(") AS _r");
        assertThat(sql.substring(rightSubqueryStart, rightSubqueryClose)).doesNotContain("WHERE");

        assertNoOuterWhere(sql);
    }

    @Test
    public void testRightSidePredicateGoesInsideRightSubquery()
    {
        StarrocksTableHandle rightWithFilter = handleWithExpressionFilter(
                RIGHT_TABLE, List.of(R_ID, R_DESC), "(`price` > 0)");

        String sql = callApplyJoin(JoinType.RIGHT_OUTER, leftHandle(), rightWithFilter);

        // Left subquery has no predicate
        int leftSubqueryClose = sql.indexOf(") AS _l");
        assertThat(sql.substring(0, leftSubqueryClose)).doesNotContain("WHERE");

        // Predicate is inside the right subquery
        int rightSubqueryStart = leftSubqueryClose + 7;
        int rightSubqueryClose = sql.indexOf(") AS _r");
        assertThat(sql.substring(rightSubqueryStart, rightSubqueryClose)).contains("WHERE");

        assertNoOuterWhere(sql);
    }

    @Test
    public void testExpressionFilterEmbeddedInLeftSubquery()
    {
        StarrocksTableHandle leftWithExpr = handleWithExpressionFilter(
                LEFT_TABLE, List.of(L_ID, L_NAME), "(`name` LIKE 'Alice%')");

        JoinApplicationResult<io.trino.spi.connector.ConnectorTableHandle> result =
                newMetadata().applyJoin(SESSION, JoinType.INNER, leftWithExpr, rightHandle(),
                        joinCondition(), leftAssignments(), rightAssignments(), emptyStats())
                        .orElseThrow();

        StarrocksTableHandle newHandle = (StarrocksTableHandle) result.getTableHandle();
        String sql = newHandle.getJoinSqlBase().orElseThrow();

        // Expression is embedded inside the left subquery
        int leftSubqueryClose = sql.indexOf(") AS _l");
        assertThat(sql.substring(0, leftSubqueryClose)).contains("LIKE 'Alice%'");

        // No outer WHERE
        assertNoOuterWhere(sql);

        // Expression filter has been consumed into the subquery — no residual on the result handle
        assertThat(newHandle.getExpressionFilter()).isEmpty();
    }

    @Test
    public void testJoinRejectedWhenLeftHasPushdownAggregates()
    {
        StarrocksTableHandle aggHandle = new StarrocksTableHandle(
                LEFT_TABLE, List.of(L_ID), TupleDomain.all(),
                Optional.empty(), Optional.empty(), Optional.empty(),
                OptionalLong.empty(), Optional.empty(),
                Optional.of(List.of(
                        new StarrocksAggregateFunction("count", Optional.empty(), false, "_agg_0", "bigint"))),
                Optional.of(List.of()));

        assertThat(newMetadata().applyJoin(SESSION, JoinType.INNER,
                aggHandle, rightHandle(), joinCondition(), leftAssignments(), rightAssignments(), emptyStats()))
                .isEmpty();
    }

    @Test
    public void testJoinRejectedWhenLeftAlreadyJoinBacked()
    {
        StarrocksTableHandle joinBacked = new StarrocksTableHandle(
                LEFT_TABLE, List.of(L_ID), TupleDomain.all(),
                Optional.empty(), Optional.empty(), Optional.empty(),
                OptionalLong.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(),
                Optional.of("SELECT * FROM `test`.`orders` AS _l INNER JOIN `test`.`items` AS _r ON _l.`id` = _r.`id`"));

        assertThat(newMetadata().applyJoin(SESSION, JoinType.INNER,
                joinBacked, rightHandle(), joinCondition(), leftAssignments(), rightAssignments(), emptyStats()))
                .isEmpty();
    }

    @Test
    public void testTupleDomainConstraintGoesInsideSubquery()
    {
        // Use a real TupleDomain constraint (not a pre-built SQL string) to verify the
        // buildPredicate path also lands inside the subquery, not in an outer WHERE.
        TupleDomain<ColumnHandle> constraint = TupleDomain.withColumnDomains(
                Map.of(L_ID, Domain.singleValue(BIGINT, 42L)));
        StarrocksTableHandle leftWithConstraint = new StarrocksTableHandle(
                LEFT_TABLE, List.of(L_ID, L_NAME), constraint,
                Optional.empty(), Optional.empty(), Optional.empty(),
                OptionalLong.empty(), Optional.empty());

        String sql = callApplyJoin(JoinType.LEFT_OUTER, leftWithConstraint, rightHandle());

        // Predicate appears inside the left subquery
        int leftSubqueryClose = sql.indexOf(") AS _l");
        assertThat(sql.substring(0, leftSubqueryClose)).contains("WHERE");

        // No outer WHERE after ON
        assertNoOuterWhere(sql);
    }

    @Test
    public void testApplyAggregationRejectedOnJoinBackedHandle()
    {
        // Guard added in Fix 3: applyAggregation must decline when the handle already
        // carries a joinSqlBase, because buildAggregateSplit reads schemaName/tableName
        // directly and would produce SQL over the wrong source.
        StarrocksTableHandle joinBacked = new StarrocksTableHandle(
                LEFT_TABLE, List.of(L_ID), TupleDomain.all(),
                Optional.empty(), Optional.empty(), Optional.empty(),
                OptionalLong.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(),
                Optional.of("SELECT * FROM `test`.`orders` AS _l INNER JOIN `test`.`items` AS _r ON _l.`id` = _r.`id`"));

        // Pass a non-empty grouping column so the independent "empty aggregates + empty
        // grouping" guard at line 649 does NOT fire — only the join-backed guard at 642 can
        // make this return empty.
        assertThat(newMetadata().applyAggregation(
                SESSION, joinBacked,
                List.of(),
                Map.of(),
                List.of(List.of(L_ID))))
                .isEmpty();
    }

    // ---- helpers ----

    private static String callApplyJoin(JoinType joinType, StarrocksTableHandle left, StarrocksTableHandle right)
    {
        JoinApplicationResult<io.trino.spi.connector.ConnectorTableHandle> result =
                newMetadata().applyJoin(SESSION, joinType, left, right,
                        joinCondition(), leftAssignments(), rightAssignments(), emptyStats())
                        .orElseThrow(() -> new AssertionError("applyJoin returned empty"));
        return ((StarrocksTableHandle) result.getTableHandle()).getJoinSqlBase().orElseThrow();
    }

    private static void assertNoOuterWhere(String sql)
    {
        int onIndex = sql.indexOf(" ON ");
        assertThat(onIndex).as("SQL must contain ON clause").isGreaterThan(0);
        assertThat(sql.substring(onIndex)).doesNotContain("WHERE");
    }

    private static StarrocksTableHandle leftHandle()
    {
        return new StarrocksTableHandle(LEFT_TABLE, List.of(L_ID, L_NAME), TupleDomain.all(),
                Optional.empty(), Optional.empty(), Optional.empty(),
                OptionalLong.empty(), Optional.empty());
    }

    private static StarrocksTableHandle rightHandle()
    {
        return new StarrocksTableHandle(RIGHT_TABLE, List.of(R_ID, R_DESC), TupleDomain.all(),
                Optional.empty(), Optional.empty(), Optional.empty(),
                OptionalLong.empty(), Optional.empty());
    }

    private static StarrocksTableHandle handleWithExpressionFilter(
            SchemaTableName table, List<StarrocksColumnHandle> cols, String expressionFilter)
    {
        return new StarrocksTableHandle(table, cols, TupleDomain.all(),
                Optional.empty(), Optional.empty(), Optional.empty(),
                OptionalLong.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(expressionFilter), Optional.empty());
    }

    private static ConnectorExpression joinCondition()
    {
        return new Call(BOOLEAN, new FunctionName("$equal"),
                List.of(new Variable("l_id", BIGINT), new Variable("r_id", BIGINT)));
    }

    private static Map<String, ColumnHandle> leftAssignments()
    {
        return Map.of("l_id", L_ID);
    }

    private static Map<String, ColumnHandle> rightAssignments()
    {
        return Map.of("r_id", R_ID);
    }

    private static JoinStatistics emptyStats()
    {
        return new JoinStatistics()
        {
            @Override
            public Optional<BasicRelationStatistics> getLeftStatistics()
            {
                return Optional.empty();
            }

            @Override
            public Optional<BasicRelationStatistics> getRightStatistics()
            {
                return Optional.empty();
            }

            @Override
            public Optional<BasicRelationStatistics> getJoinStatistics()
            {
                return Optional.empty();
            }
        };
    }

    private static StarrocksColumnHandle col(String name, int ordinal, String type)
    {
        return new StarrocksColumnHandle(name, ordinal, type, type, true, "", "", 0, 0);
    }

    private static StarrocksMetadata newMetadata()
    {
        StarrocksFEClient feClient = new StarrocksFEClient(CONFIG, "starrocks", null, null);
        StarrocksClient client = new StarrocksClient(CONFIG, feClient, new StarrocksBEClient(CONFIG));
        return new StarrocksMetadata(client, CONFIG, new StarrocksTypeMapper(TESTING_TYPE_MANAGER));
    }
}
