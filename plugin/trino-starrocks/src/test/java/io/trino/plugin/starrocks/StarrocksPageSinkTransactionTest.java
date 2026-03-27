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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class StarrocksPageSinkTransactionTest
{
    @Test
    public void testFinishUsesTransactionalCommitFlow()
            throws Exception
    {
        AtomicInteger beginCalls = new AtomicInteger();
        AtomicInteger loadCalls = new AtomicInteger();
        AtomicInteger prepareCalls = new AtomicInteger();
        AtomicInteger commitCalls = new AtomicInteger();
        AtomicInteger rollbackCalls = new AtomicInteger();

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/transaction/begin", exchange -> {
            beginCalls.incrementAndGet();
            writeJson(exchange, 200, "{\"Status\":\"OK\",\"TxnId\":1}");
        });
        server.createContext("/api/transaction/load", exchange -> {
            loadCalls.incrementAndGet();
            writeJson(exchange, 200, "{\"Status\":\"OK\",\"NumberFilteredRows\":0}");
        });
        server.createContext("/api/transaction/prepare", exchange -> {
            prepareCalls.incrementAndGet();
            writeJson(exchange, 200, "{\"Status\":\"OK\"}");
        });
        server.createContext("/api/transaction/commit", exchange -> {
            commitCalls.incrementAndGet();
            writeJson(exchange, 200, "{\"Status\":\"OK\"}");
        });
        server.createContext("/api/transaction/rollback", exchange -> {
            rollbackCalls.incrementAndGet();
            writeJson(exchange, 200, "{\"Status\":\"OK\"}");
        });
        server.start();

        try {
            StarrocksPageSink sink = newSink(server.getAddress().getPort(), 2);
            sink.appendPage(pageWithValues("alice", "bob", "carol"));
            Collection<Slice> result = sink.finish().join();

            assertThat(result).isEmpty();
            assertThat(beginCalls.get()).isEqualTo(1);
            assertThat(loadCalls.get()).isEqualTo(2);
            assertThat(prepareCalls.get()).isEqualTo(1);
            assertThat(commitCalls.get()).isEqualTo(1);
            assertThat(rollbackCalls.get()).isEqualTo(0);
        }
        finally {
            server.stop(0);
        }
    }

    @Test
    public void testAbortRollsBackTransaction()
            throws Exception
    {
        AtomicInteger beginCalls = new AtomicInteger();
        AtomicInteger loadCalls = new AtomicInteger();
        AtomicInteger rollbackCalls = new AtomicInteger();

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/transaction/begin", exchange -> {
            beginCalls.incrementAndGet();
            writeJson(exchange, 200, "{\"Status\":\"OK\",\"TxnId\":1}");
        });
        server.createContext("/api/transaction/load", exchange -> {
            loadCalls.incrementAndGet();
            writeJson(exchange, 200, "{\"Status\":\"OK\",\"NumberFilteredRows\":0}");
        });
        server.createContext("/api/transaction/rollback", exchange -> {
            rollbackCalls.incrementAndGet();
            writeJson(exchange, 200, "{\"Status\":\"OK\"}");
        });
        server.createContext("/api/transaction/prepare", exchange -> writeJson(exchange, 200, "{\"Status\":\"OK\"}"));
        server.createContext("/api/transaction/commit", exchange -> writeJson(exchange, 200, "{\"Status\":\"OK\"}"));
        server.start();

        try {
            StarrocksPageSink sink = newSink(server.getAddress().getPort(), 1);
            sink.appendPage(pageWithValues("a"));
            sink.abort();

            assertThat(beginCalls.get()).isEqualTo(1);
            assertThat(loadCalls.get()).isEqualTo(1);
            assertThat(rollbackCalls.get()).isEqualTo(1);
        }
        finally {
            server.stop(0);
        }
    }

    @Test
    public void testCommitFailureTriggersRollback()
            throws Exception
    {
        AtomicInteger rollbackCalls = new AtomicInteger();

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/transaction/begin", exchange -> writeJson(exchange, 200, "{\"Status\":\"OK\",\"TxnId\":1}"));
        server.createContext("/api/transaction/load", exchange -> writeJson(exchange, 200, "{\"Status\":\"OK\",\"NumberFilteredRows\":0}"));
        server.createContext("/api/transaction/prepare", exchange -> writeJson(exchange, 200, "{\"Status\":\"OK\"}"));
        server.createContext("/api/transaction/commit", exchange -> writeJson(exchange, 200, "{\"Status\":\"FAILED\",\"Message\":\"commit timeout\"}"));
        server.createContext("/api/transaction/rollback", exchange -> {
            rollbackCalls.incrementAndGet();
            writeJson(exchange, 200, "{\"Status\":\"OK\"}");
        });
        server.start();

        try {
            StarrocksPageSink sink = newSink(server.getAddress().getPort(), 1);
            sink.appendPage(pageWithValues("a"));

            assertThatThrownBy(sink::finish)
                    .isInstanceOf(TrinoException.class)
                    .hasMessageContaining("Failed to commit StarRocks stream-load transaction");
            assertThat(rollbackCalls.get()).isEqualTo(1);
        }
        finally {
            server.stop(0);
        }
    }

    private static StarrocksPageSink newSink(int port, int batchRows)
    {
        StarrocksConfig config = new StarrocksConfig()
                .setScanURL("http://127.0.0.1:" + port)
                .setJdbcURL("jdbc:mysql://127.0.0.1:9030")
                .setUsername("root")
                .setPassword("")
                .setScanBatchRows(batchRows);

        StarrocksInsertTableHandle handle = new StarrocksInsertTableHandle(
                "db1",
                "tbl1",
                List.of("name"),
                List.of(VARCHAR));

        return new StarrocksPageSink(config, handle, "query_1", 1L);
    }

    private static Page pageWithValues(String... values)
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, values.length);
        for (String value : values) {
            VARCHAR.writeString(blockBuilder, value);
        }
        return new Page(values.length, blockBuilder.build());
    }

    private static void writeJson(HttpExchange exchange, int status, String body)
            throws IOException
    {
        byte[] payload = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, payload.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(payload);
        }
        exchange.close();
    }
}
