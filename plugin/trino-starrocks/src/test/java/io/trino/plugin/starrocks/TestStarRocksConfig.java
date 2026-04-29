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

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

final class TestStarRocksConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(StarRocksConfig.class)
                .setJdbcUrl(null)
                .setCatalogName(null)
                .setUsername(null)
                .setPassword(null)
                .setFlightSqlHost(null)
                .setFlightSqlPort(9408));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = Map.ofEntries(
                Map.entry("starrocks.jdbc-url", "jdbc:starrocks://fe.example.net:9030"),
                Map.entry("starrocks.catalog-name", "external_catalog"),
                Map.entry("starrocks.username", "trino"),
                Map.entry("starrocks.password", "secret"),
                Map.entry("starrocks.flight-sql-host", "flight.example.net"),
                Map.entry("starrocks.flight-sql-port", "9500"));

        StarRocksConfig expected = new StarRocksConfig()
                .setJdbcUrl("jdbc:starrocks://fe.example.net:9030")
                .setCatalogName("external_catalog")
                .setUsername("trino")
                .setPassword("secret")
                .setFlightSqlHost("flight.example.net")
                .setFlightSqlPort(9500);

        assertFullMapping(properties, expected);
    }
}
