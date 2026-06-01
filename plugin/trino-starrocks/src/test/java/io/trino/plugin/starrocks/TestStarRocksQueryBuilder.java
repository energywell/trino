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

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static io.trino.spi.predicate.ValueSet.ofRanges;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;

final class TestStarRocksQueryBuilder
{
    @Test
    void testBuildSqlUsesRemoteNames()
    {
        StarRocksQueryBuilder queryBuilder = new StarRocksQueryBuilder();
        StarRocksTableHandle tableHandle = new StarRocksTableHandle("sales", "orders", Optional.empty(), "Sales", "Orders", StarRocksRelationType.TABLE);

        assertThat(queryBuilder.buildSelectSql(
                tableHandle,
                List.of(
                        new StarRocksColumnHandle("orderkey", "OrderKey", BIGINT, 0),
                        new StarRocksColumnHandle("customer_name", "Customer``Name", createVarcharType(20), 1))))
                .isEqualTo("SELECT `OrderKey` AS `orderkey`, `Customer````Name` AS `customer_name` FROM `Sales`.`Orders`");
    }

    @Test
    void testBuildSelectSqlWithEmptyProjection()
    {
        StarRocksQueryBuilder queryBuilder = new StarRocksQueryBuilder();

        assertThat(queryBuilder.buildSelectSql(
                new StarRocksTableHandle("sales", "orders", Optional.empty(), "sales", "orders", StarRocksRelationType.TABLE),
                List.of()))
                .isEqualTo("SELECT 1 FROM `sales`.`orders`");
    }

    @Test
    void testBuildSelectSqlWithCatalogPredicateSortAndLimit()
    {
        StarRocksQueryBuilder queryBuilder = new StarRocksQueryBuilder();
        StarRocksColumnHandle orderKey = new StarRocksColumnHandle("orderkey", "OrderKey", BIGINT, 0);
        StarRocksTableHandle tableHandle = new StarRocksTableHandle("sales", "orders", Optional.of("external_catalog"), "Sales", "Orders", StarRocksRelationType.TABLE)
                .withConstraint(withColumnDomains(Map.of(orderKey, Domain.create(ofRanges(range(BIGINT, 10L, true, 20L, false)), false))))
                .withTopN(5, List.of(new StarRocksSortItem("orderkey", "OrderKey", ASC_NULLS_LAST)));

        assertThat(queryBuilder.buildSelectSql(tableHandle, List.of(orderKey)))
                .isEqualTo("SELECT `OrderKey` AS `orderkey` FROM `external_catalog`.`Sales`.`Orders` WHERE (`OrderKey` >= 10 AND `OrderKey` < 20) ORDER BY `OrderKey` IS NULL ASC, `OrderKey` ASC LIMIT 5");
    }

    @Test
    void testBuildSelectSqlWithAggregation()
    {
        StarRocksQueryBuilder queryBuilder = new StarRocksQueryBuilder();
        StarRocksColumnHandle aggregate = new StarRocksColumnHandle("_starrocks_agg_0", "_starrocks_agg_0", BIGINT, 0);
        StarRocksTableHandle tableHandle = new StarRocksTableHandle("sales", "orders", Optional.empty(), "sales", "orders", StarRocksRelationType.TABLE)
                .withAggregation(new StarRocksAggregation(
                        List.of(),
                        List.of(new StarRocksAggregateColumn("_starrocks_agg_0", "count(*)", BIGINT))));

        assertThat(queryBuilder.buildSelectSql(tableHandle, List.of(aggregate)))
                .isEqualTo("SELECT count(*) AS `_starrocks_agg_0` FROM `sales`.`orders`");
    }

    @Test
    void testUnsupportedFloatingPointPredicateLiteralIsNotPushedDown()
    {
        StarRocksColumnHandle ratio = new StarRocksColumnHandle("ratio", "ratio", DOUBLE, 0);

        assertThat(StarRocksQueryBuilder.buildColumnPredicate(ratio, Domain.singleValue(DOUBLE, Double.POSITIVE_INFINITY)))
                .isEmpty();
    }

    @Test
    void testNotEqualVarcharPushdown()
    {
        StarRocksColumnHandle source = new StarRocksColumnHandle("source", "Source", createVarcharType(50), 0);
        // source != 'ecoflow', NULLs not allowed
        Domain domain = Domain.create(ValueSet.of(createVarcharType(50), utf8Slice("ecoflow")).complement(), false);

        assertThat(StarRocksQueryBuilder.buildColumnPredicate(source, domain))
                .hasValue("`Source` != 'ecoflow'");
    }

    @Test
    void testNotInVarcharPushdown()
    {
        StarRocksColumnHandle source = new StarRocksColumnHandle("source", "Source", createVarcharType(50), 0);
        // source NOT IN ('alpha', 'beta'), NULLs not allowed
        Domain domain = Domain.create(ValueSet.of(createVarcharType(50), utf8Slice("alpha"), utf8Slice("beta")).complement(), false);

        assertThat(StarRocksQueryBuilder.buildColumnPredicate(source, domain))
                .hasValue("`Source` NOT IN ('alpha', 'beta')");
    }

    @Test
    void testNotEqualVarcharPushdownWithNullAllowed()
    {
        StarRocksColumnHandle source = new StarRocksColumnHandle("source", "Source", createVarcharType(50), 0);
        // source != 'ecoflow' OR source IS NULL
        Domain domain = Domain.create(ValueSet.of(createVarcharType(50), utf8Slice("ecoflow")).complement(), true);

        assertThat(StarRocksQueryBuilder.buildColumnPredicate(source, domain))
                .hasValue("(`Source` != 'ecoflow' OR `Source` IS NULL)");
    }

    @Test
    void testTemporalPredicatesUseCast()
    {
        StarRocksColumnHandle created = new StarRocksColumnHandle("created", "Created", createTimestampType(0), 0);
        Domain domain = Domain.create(ofRanges(range(createTimestampType(0), 1_700_000_000_000_000L, true, 1_800_000_000_000_000L, false)), false);

        assertThat(StarRocksQueryBuilder.buildColumnPredicate(created, domain))
                .hasValueSatisfying(sql -> assertThat(sql)
                        .contains("CAST(")
                        .contains("AS DATETIME)")
                        .doesNotContain("TIMESTAMP '"));
    }
}
