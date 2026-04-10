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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStarrocksPlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new StarrocksPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThat(factory.getName()).isEqualTo("starrocks");

        factory.create(
                "test",
                ImmutableMap.of(
                        "jdbc-url", "jdbc:mysql://starrocks.example.com:9030",
                        "scan-url", "starrocks.example.com:8030",
                        "username", "root",
                        "bootstrap.quiet", "true"),
                new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testMissingRequiredConfig()
    {
        Plugin plugin = new StarrocksPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());

        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.of(
                        "scan-url", "starrocks.example.com:8030",
                        "username", "root",
                        "bootstrap.quiet", "true"),
                new TestingConnectorContext()))
                .hasMessageContaining("jdbc-url");

        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.of(
                        "jdbc-url", "jdbc:mysql://starrocks.example.com:9030",
                        "username", "root",
                        "bootstrap.quiet", "true"),
                new TestingConnectorContext()))
                .hasMessageContaining("scan-url");

        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.of(
                        "jdbc-url", "jdbc:mysql://starrocks.example.com:9030",
                        "scan-url", "starrocks.example.com:8030",
                        "bootstrap.quiet", "true"),
                new TestingConnectorContext()))
                .hasMessageContaining("username");
    }
}
