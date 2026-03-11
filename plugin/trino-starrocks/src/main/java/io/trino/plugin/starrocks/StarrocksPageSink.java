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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static okhttp3.Credentials.basic;

public class StarrocksPageSink
        implements ConnectorPageSink
{
    private static final MediaType JSON_MEDIA_TYPE = MediaType.parse("application/json");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final int MAX_BUFFERED_ROWS = 10_000;

    private final StarrocksConfig config;
    private final String schemaName;
    private final String tableName;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final List<Map<String, Object>> bufferedRows;
    private final OkHttpClient httpClient;
    private final String streamLoadLabelPrefix;
    private long streamLoadSequence;

    public StarrocksPageSink(StarrocksConfig config, StarrocksInsertTableHandle handle, String queryId, long pageSinkId)
    {
        this(config, handle.getSchemaName(), handle.getTableName(), handle.getColumnNames(), handle.getColumnTypes(), queryId, pageSinkId);
    }

    public StarrocksPageSink(StarrocksConfig config, StarrocksOutputTableHandle handle, String queryId, long pageSinkId)
    {
        this(config, handle.getSchemaName(), handle.getTemporaryTableName(), handle.getColumnNames(), handle.getColumnTypes(), queryId, pageSinkId);
    }

    private StarrocksPageSink(
            StarrocksConfig config,
            String schemaName,
            String tableName,
            List<String> columnNames,
            List<Type> columnTypes,
            String queryId,
            long pageSinkId)
    {
        this.config = requireNonNull(config, "config is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columnNames = List.copyOf(requireNonNull(columnNames, "columnNames is null"));
        this.columnTypes = List.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        this.bufferedRows = new ArrayList<>();
        this.streamLoadLabelPrefix = buildStreamLoadLabelPrefix(requireNonNull(queryId, "queryId is null"), pageSinkId);
        this.streamLoadSequence = 0;
        // StarRocks FE may return 307 redirecting PUT to a BE node.
        // OkHttp by default may change PUT to GET on redirect, so we handle 307 manually.
        this.httpClient = new OkHttpClient.Builder()
                .followRedirects(false)
                .addInterceptor(chain -> {
                    Response response = chain.proceed(chain.request());
                    if (response.code() == 307) {
                        String location = response.header("Location");
                        if (location != null) {
                            response.close();
                            Request redirect = chain.request().newBuilder()
                                    .url(location)
                                    .build();
                            return chain.proceed(redirect);
                        }
                    }
                    return response;
                })
                .build();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        for (int position = 0; position < page.getPositionCount(); position++) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Block block = page.getBlock(channel);
                String columnName = columnNames.get(channel);
                Type type = columnTypes.get(channel);
                row.put(columnName, extractValue(type, block, position));
            }
            bufferedRows.add(row);
            if (bufferedRows.size() >= MAX_BUFFERED_ROWS) {
                try {
                    flushBufferedRows();
                }
                catch (IOException e) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR,
                            "Failed to send Stream Load to StarRocks: " + e.getMessage(), e);
                }
            }
        }
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        if (bufferedRows.isEmpty()) {
            return completedFuture(List.of());
        }
        try {
            flushBufferedRows();
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    "Failed to send Stream Load to StarRocks: " + e.getMessage(), e);
        }
        return completedFuture(List.of());
    }

    @Override
    public void abort()
    {
        bufferedRows.clear();
    }

    private void flushBufferedRows()
            throws IOException
    {
        if (bufferedRows.isEmpty()) {
            return;
        }
        sendStreamLoad(bufferedRows, streamLoadLabelPrefix + "_" + streamLoadSequence++);
        bufferedRows.clear();
    }

    @SuppressWarnings("unchecked")
    private void sendStreamLoad(List<Map<String, Object>> rows, String label)
            throws IOException
    {
        String[] httpNodes = config.getScanURL().split(",");
        String httpNode = httpNodes[new Random().nextInt(httpNodes.length)].trim();
        String url = String.format("http://%s/api/%s/%s/_stream_load",
                httpNode, schemaName, tableName);

        String jsonBody = OBJECT_MAPPER.writeValueAsString(rows);
        String columns = String.join(",", columnNames);
        String credential = basic(config.getUsername(), config.getPassword().orElse(""));

        Request request = new Request.Builder()
                .url(url)
                .put(RequestBody.create(jsonBody, JSON_MEDIA_TYPE))
                .header("Authorization", credential)
                .header("format", "json")
                .header("strip_outer_array", "true")
                .header("columns", columns)
                .header("label", label)
                .header("Expect", "100-continue")
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            String responseBody = response.body() != null ? response.body().string() : "";
            Map<String, Object> result = Map.of();
            if (!responseBody.isBlank()) {
                try {
                    result = OBJECT_MAPPER.readValue(responseBody, Map.class);
                }
                catch (JsonProcessingException ignored) {
                    // Keep empty result. The error is still surfaced via HTTP status handling below.
                }
            }

            String status = asString(result.get("Status"));
            String message = asString(result.get("Message"));
            if (isLabelAlreadyExists(status, message)) {
                return;
            }

            if (!response.isSuccessful()) {
                throw new IOException("Stream Load failed with HTTP " + response.code() + ": " + responseBody);
            }
            if (!"Success".equals(status) && !"Publish Timeout".equals(status)) {
                throw new IOException("Stream Load failed with status: " + status + ", message: " + message);
            }

            long filteredRows = asLong(result.get("NumberFilteredRows"));
            if (filteredRows > 0) {
                throw new IOException("Stream Load filtered " + filteredRows + " rows. response: " + responseBody);
            }
        }
    }

    private Object extractValue(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (type instanceof BooleanType) {
            return type.getBoolean(block, position);
        }
        if (type instanceof TinyintType) {
            return (byte) type.getLong(block, position);
        }
        if (type instanceof SmallintType) {
            return (short) type.getLong(block, position);
        }
        if (type instanceof IntegerType) {
            return (int) type.getLong(block, position);
        }
        if (type instanceof BigintType) {
            return type.getLong(block, position);
        }
        if (type instanceof RealType) {
            return ((RealType) type).getFloat(block, position);
        }
        if (type instanceof DoubleType) {
            return type.getDouble(block, position);
        }
        if (type instanceof DecimalType) {
            return type.getObjectValue(null, block, position).toString();
        }
        if (type instanceof VarcharType) {
            return type.getSlice(block, position).toStringUtf8();
        }
        if (type instanceof VarbinaryType) {
            return Base64.getEncoder().encodeToString(type.getSlice(block, position).getBytes());
        }
        if (type.getBaseName().equals(StandardTypes.JSON)) {
            // Parse to JsonNode so Jackson embeds raw JSON, not a double-escaped string
            String jsonString = type.getSlice(block, position).toStringUtf8();
            try {
                return OBJECT_MAPPER.readTree(jsonString);
            }
            catch (JsonProcessingException e) {
                return jsonString;
            }
        }
        if (type instanceof DateType || type instanceof TimestampType) {
            return type.getObjectValue(null, block, position).toString();
        }
        if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
            return type.getObjectValue(null, block, position);
        }
        // Fallback
        Object value = type.getObjectValue(null, block, position);
        return value != null ? value.toString() : null;
    }

    private static String buildStreamLoadLabelPrefix(String queryId, long pageSinkId)
    {
        String normalizedQueryId = queryId.replaceAll("[^A-Za-z0-9_]", "_");
        return "trino_" + normalizedQueryId + "_" + pageSinkId;
    }

    private static String asString(Object value)
    {
        return value == null ? "" : value.toString();
    }

    private static long asLong(Object value)
    {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        try {
            return Long.parseLong(value.toString());
        }
        catch (NumberFormatException ignored) {
            return 0;
        }
    }

    private static boolean isLabelAlreadyExists(String status, String message)
    {
        return "Label Already Exists".equalsIgnoreCase(status)
                || (message != null && message.toLowerCase().contains("label") && message.toLowerCase().contains("exist"));
    }
}
