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
import io.trino.spi.connector.ConnectorSession;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class AdbcStarRocksFlightSqlClient
        implements StarRocksFlightSqlClient
{
    private static final String APPLICATION_NAME_PREFIX = "/* ApplicationName=Trino StarRocks Flight SQL Query */ ";

    private final String flightSqlHost;
    private final int flightSqlPort;
    private final Optional<String> username;
    private final Optional<String> password;
    private final StarRocksQueryBuilder queryBuilder;

    @Inject
    public AdbcStarRocksFlightSqlClient(StarRocksConfig config, StarRocksQueryBuilder queryBuilder)
    {
        requireNonNull(config, "config is null");
        this.flightSqlHost = config.getFlightSqlHost()
                .orElseGet(() -> inferFlightSqlHost(config.getJdbcUrl()
                        .orElseThrow(() -> new IllegalArgumentException("starrocks.jdbc-url must be set"))));
        this.flightSqlPort = config.getFlightSqlPort();
        this.username = config.getUsername();
        this.password = config.getPassword();
        this.queryBuilder = requireNonNull(queryBuilder, "queryBuilder is null");
    }

    @Override
    public StarRocksFlightSqlResult openStream(
            ConnectorSession session,
            StarRocksTableHandle tableHandle,
            StarRocksSplit split,
            List<StarRocksColumnHandle> columns)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(split, "split is null");
        requireNonNull(columns, "columns is null");

        String sql = APPLICATION_NAME_PREFIX + queryBuilder.buildSelectSql(tableHandle, columns);
        return openStream(sql);
    }

    private StarRocksFlightSqlResult openStream(String sql)
    {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        AdbcDatabase database = null;
        AdbcConnection connection = null;
        AdbcStatement statement = null;
        AdbcStatement.QueryResult queryResult = null;
        ArrowReader reader = null;

        try {
            FlightSqlDriver driver = new FlightSqlDriver(allocator);
            Map<String, Object> parameters = new HashMap<>();
            AdbcDriver.PARAM_URI.set(parameters, Location.forGrpcInsecure(flightSqlHost, flightSqlPort).getUri().toString());
            username.ifPresent(value -> AdbcDriver.PARAM_USERNAME.set(parameters, value));
            password.ifPresent(value -> AdbcDriver.PARAM_PASSWORD.set(parameters, value));

            database = driver.open(parameters);
            connection = database.connect();
            statement = connection.createStatement();
            statement.setSqlQuery(sql);
            queryResult = statement.executeQuery();
            reader = queryResult.getReader();

            return new AdbcResult(allocator, database, connection, statement, queryResult, reader);
        }
        catch (AdbcException | RuntimeException e) {
            closeQuietly(reader);
            closeQuietly(queryResult);
            closeQuietly(statement);
            closeQuietly(connection);
            closeQuietly(database);
            closeQuietly(allocator);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to execute StarRocks Flight SQL query", e);
        }
    }

    private static String inferFlightSqlHost(String jdbcUrl)
    {
        try {
            URI uri = URI.create(jdbcUrl.substring("jdbc:".length()));
            if (uri.getHost() == null || uri.getHost().isBlank()) {
                throw new IllegalArgumentException("Unable to infer host from JDBC URL: " + jdbcUrl);
            }
            return uri.getHost();
        }
        catch (RuntimeException e) {
            throw new IllegalArgumentException("Unable to infer StarRocks Flight SQL host from JDBC URL: " + jdbcUrl, e);
        }
    }

    private static void closeQuietly(AutoCloseable closeable)
    {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        }
        catch (Exception _) {
            // Cleanup failures are ignored so the original query failure is preserved.
        }
    }

    private static final class AdbcResult
            implements StarRocksFlightSqlResult
    {
        private final BufferAllocator allocator;
        private final AdbcDatabase database;
        private final AdbcConnection connection;
        private final AdbcStatement statement;
        private final AdbcStatement.QueryResult queryResult;
        private final ArrowReader reader;
        private final AtomicBoolean closed = new AtomicBoolean();

        private AdbcResult(
                BufferAllocator allocator,
                AdbcDatabase database,
                AdbcConnection connection,
                AdbcStatement statement,
                AdbcStatement.QueryResult queryResult,
                ArrowReader reader)
        {
            this.allocator = requireNonNull(allocator, "allocator is null");
            this.database = requireNonNull(database, "database is null");
            this.connection = requireNonNull(connection, "connection is null");
            this.statement = requireNonNull(statement, "statement is null");
            this.queryResult = requireNonNull(queryResult, "queryResult is null");
            this.reader = requireNonNull(reader, "reader is null");
        }

        @Override
        public boolean loadNextBatch()
        {
            if (closed.get()) {
                return false;
            }

            try {
                return reader.loadNextBatch();
            }
            catch (Exception e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to load StarRocks Flight SQL batch", e);
            }
        }

        @Override
        public VectorSchemaRoot getVectorSchemaRoot()
        {
            try {
                return reader.getVectorSchemaRoot();
            }
            catch (Exception e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to access StarRocks Flight SQL batch schema root", e);
            }
        }

        @Override
        public long getMemoryUsage()
        {
            if (closed.get()) {
                return 0;
            }
            try {
                return allocator.getAllocatedMemory();
            }
            catch (RuntimeException ignored) {
                return 0;
            }
        }

        @Override
        public void close()
        {
            if (!closed.compareAndSet(false, true)) {
                return;
            }

            Exception failure = null;
            failure = closeResource(failure, reader);
            failure = closeResource(failure, queryResult);
            failure = closeResource(failure, statement);
            failure = closeResource(failure, connection);
            failure = closeResource(failure, database);
            failure = closeResource(failure, allocator);

            if (failure != null) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to close StarRocks Flight SQL result", failure);
            }
        }

        private static Exception closeResource(Exception failure, AutoCloseable closeable)
        {
            try {
                closeable.close();
            }
            catch (Exception e) {
                if (failure == null) {
                    return e;
                }
                failure.addSuppressed(e);
            }
            return failure;
        }
    }
}
