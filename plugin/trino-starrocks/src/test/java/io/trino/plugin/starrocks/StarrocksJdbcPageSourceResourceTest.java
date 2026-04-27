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

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class StarrocksJdbcPageSourceResourceTest
{
    private static final StarrocksConfig CONFIG = new StarrocksConfig()
            .setJdbcURL("jdbc:mysql://localhost:9030")
            .setScanURL("localhost:8030")
            .setUsername("starrocks")
            .setPassword("");

    @Test
    public void testConstructorClosesStatementWhenExecuteQueryFails()
    {
        AtomicBoolean statementClosed = new AtomicBoolean();

        Statement fakeStatement = proxyStatement(
                /* throwOnExecute */ true,
                /* onClose */ () -> statementClosed.set(true));

        Connection fakeConnection = proxyConnection(fakeStatement, /* onClose */ () -> {});

        assertThatThrownBy(() -> new StarrocksJdbcPageSource(
                fakeConnection, "SELECT 1", List.of(), new StarrocksTypeMapper(TESTING_TYPE_MANAGER)))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Failed to execute aggregate query");

        assertThat(statementClosed.get())
                .as("statement must be closed when executeQuery fails")
                .isTrue();
    }

    @Test
    public void testProviderClosesConnectionWhenPageSourceConstructorFails()
    {
        AtomicBoolean connectionClosed = new AtomicBoolean();
        AtomicBoolean statementClosed = new AtomicBoolean();

        Statement fakeStatement = proxyStatement(
                /* throwOnExecute */ true,
                /* onClose */ () -> statementClosed.set(true));

        Connection fakeConnection = proxyConnection(
                fakeStatement,
                /* onClose */ () -> connectionClosed.set(true));

        StarrocksFEClient fakeFeClient = new StarrocksFEClient(CONFIG, "starrocks", null, null)
        {
            @Override
            public Connection openConnection(ConnectorSession session)
            {
                return fakeConnection;
            }
        };

        StarrocksClient client = new StarrocksClient(CONFIG, fakeFeClient, new StarrocksBEClient(CONFIG));
        StarrocksPageSourceProvider provider = new StarrocksPageSourceProvider(client, new StarrocksTypeMapper(TESTING_TYPE_MANAGER));

        StarrocksTableHandle tableHandle = new StarrocksTableHandle(
                new SchemaTableName("test", "t"),
                List.of(), TupleDomain.all(),
                Optional.empty(), Optional.empty(), Optional.empty(),
                OptionalLong.empty(), Optional.empty());

        assertThatThrownBy(() -> provider.createPageSource(
                null, SESSION,
                new StarrocksAggregateSplit("SELECT 1"),
                tableHandle, List.of(), DynamicFilter.EMPTY))
                .isInstanceOf(TrinoException.class);

        assertThat(connectionClosed.get())
                .as("connection must be closed when page source constructor fails")
                .isTrue();
        assertThat(statementClosed.get())
                .as("statement must be closed when executeQuery fails inside constructor")
                .isTrue();
    }

    // ---- helpers ----

    private static Statement proxyStatement(boolean throwOnExecute, Runnable onClose)
    {
        return (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class[] {Statement.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "executeQuery" -> {
                        if (throwOnExecute) {
                            throw new SQLException("simulated executeQuery failure");
                        }
                        yield null;
                    }
                    case "close" -> {
                        onClose.run();
                        yield null;
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Connection proxyConnection(Statement statement, Runnable onClose)
    {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class[] {Connection.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "createStatement" -> statement;
                    case "close" -> {
                        onClose.run();
                        yield null;
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
