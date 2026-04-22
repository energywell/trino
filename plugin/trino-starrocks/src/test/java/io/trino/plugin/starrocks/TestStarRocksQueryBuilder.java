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

import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;

final class TestStarRocksQueryBuilder
{
    @Test
    void testBuildSqlUsesRemoteNames()
    {
        StarRocksQueryBuilder queryBuilder = new StarRocksQueryBuilder();
        StarRocksTableHandle tableHandle = new StarRocksTableHandle("sales", "orders", "Sales", "Orders", StarRocksRelationType.TABLE);

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
                new StarRocksTableHandle("sales", "orders", "sales", "orders", StarRocksRelationType.TABLE),
                List.of()))
                .isEqualTo("SELECT 1 FROM `sales`.`orders`");
    }
}
