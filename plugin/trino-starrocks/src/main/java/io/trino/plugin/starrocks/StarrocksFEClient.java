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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.mysql.cj.jdbc.Driver;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.StaticCredentialProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.EquatableValueSet;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static java.util.Objects.requireNonNull;
import static okhttp3.Credentials.basic;

public class StarrocksFEClient
{
    static final int DomainLimit = 1000;
    private static final int DEFAULT_BUCKETS = 10;
    static final Logger LOG = LoggerFactory.getLogger(StarrocksFEClient.class);
    private static final String INFORMATION_SCHEMA = "information_schema";
    private static final String STATISTICS_SCHEMA = "_statistics_";
    private static final String SYSTEM_SCHEMA = "sys";
    private static final List<String> BUILD_IN_DATABASES =
            ImmutableList.of(
                    INFORMATION_SCHEMA,
                    STATISTICS_SCHEMA,
                    SYSTEM_SCHEMA);
    private final StarrocksConfig config;
    private final String catalogName;
    private final DriverConnectionFactory dbClient;
    private final OkHttpClient httpClient;

    public StarrocksFEClient(StarrocksConfig config, StarrocksCatalogName catalogName)
    {
        this(
                config,
                catalogName.getCatalogName(),
                createDbClient(config),
                createHttpClient(config));
    }

    StarrocksFEClient(StarrocksConfig config, String catalogName, DriverConnectionFactory dbClient, OkHttpClient httpClient)
    {
        this.config = requireNonNull(config, "config is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.dbClient = dbClient;
        this.httpClient = httpClient;
    }

    private static DriverConnectionFactory createDbClient(StarrocksConfig config)
    {
        String dbUrl = String.valueOf(config.getJdbcURL());
        Properties proper = new Properties();
        proper.put("useSSL", "false");
        proper.put("user", config.getUsername());
        proper.put("password", config.getPassword().orElse(""));
        CredentialProvider authenticator = new StaticCredentialProvider(
                Optional.of(config.getUsername()),
                config.getPassword());
        try {
            Driver driver = new Driver();
            return DriverConnectionFactory.builder(driver, dbUrl, authenticator)
                    .setConnectionProperties(proper)
                    .setOpenTelemetry(OpenTelemetry.noop())
                    .build();
        }
        catch (SQLException e) {
            LOG.error("Create DBClient fail", e);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to initialize StarRocks JDBC client: " + e.getMessage(), e);
        }
    }

    private static OkHttpClient createHttpClient(StarrocksConfig config)
    {
        Builder clientBuilder = new Builder();
        // add basic auth
        clientBuilder.setAuthenticator$okhttp(
                (route, response) -> response.request().newBuilder()
                        .header("Authorization", basic(config.getUsername(), config.getPassword().orElse("")))
                        .build());
        return clientBuilder.build();
    }

    private static StarrocksQueryPlan getQueryPlan(
            String querySQL,
            String httpNode,
            StarrocksConfig config,
            StarrocksTableHandle tableHandle)
            throws IOException
    {
        String url = new StringBuilder("http://")
                .append(httpNode)
                .append("/api/")
                .append(tableHandle.getSchemaTableName().getSchemaName())
                .append("/")
                .append(tableHandle.getSchemaTableName().getTableName())
                .append("/_query_plan")
                .toString();

        Map<String, Object> bodyMap = new HashMap<>();
        bodyMap.put("sql", querySQL);
        String body = new JSONObject(bodyMap).toString();
        int requsetCode = 0;
        String respString = "";
        for (int i = 0; i < config.getScanMaxRetries(); i++) {
            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                HttpPost post = new HttpPost(url);
                post.setHeader("Content-Type", "application/json;charset=UTF-8");
                post.setHeader("Authorization", basic(config.getUsername(), config.getPassword().orElse(null)));
                post.setEntity(new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8)));
                try (CloseableHttpResponse response = httpClient.execute(post)) {
                    requsetCode = response.getStatusLine().getStatusCode();
                    HttpEntity respEntity = response.getEntity();
                    respString = EntityUtils.toString(respEntity, "UTF-8");
                }
            }
            if (HttpURLConnection.HTTP_OK == requsetCode || i == config.getScanMaxRetries() - 1) {
                break;
            }
            LOG.warn("Request of get query plan failed with code:{}", requsetCode);
            try {
                Thread.sleep(1000L * (i + 1));
            }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IOException("Unable to get query plan, interrupted while doing another attempt", ex);
            }
        }
        if (200 != requsetCode) {
            throw new IOException("Request of get query plan failed with code " + requsetCode + " " + respString);
        }
        if (respString.isEmpty() || respString.equals("")) {
            LOG.warn("Request failed with empty response.");
            throw new IOException("Request failed with empty response, code: " + requsetCode);
        }
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            JsonNode rootNode = objectMapper.readTree(respString);
            if (rootNode.has("data")) {
                respString = rootNode.get("data").toString();
            }
            return objectMapper.readValue(respString, StarrocksQueryPlan.class);
        }
        catch (IOException e) {
            LOG.error("Parse response failed", e);
            throw new IOException("Parse response failed: " + e.getMessage(), e);
        }
    }

    private static Map<String, Set<Long>> transferQueryPlanToBeXTablet(StarrocksQueryPlan queryPlan)
    {
        Map<String, Set<Long>> beXTablets = new HashMap<>();
        queryPlan.getPartitions().forEach((tabletId, routingList) -> {
            int tabletCount = Integer.MAX_VALUE;
            String candidateBe = "";
            for (String beNode : routingList.getRoutings()) {
                if (!beXTablets.containsKey(beNode)) {
                    beXTablets.put(beNode, new HashSet<>());
                    candidateBe = beNode;
                    break;
                }
                if (beXTablets.get(beNode).size() < tabletCount) {
                    candidateBe = beNode;
                    tabletCount = beXTablets.get(beNode).size();
                }
            }
            beXTablets.get(candidateBe).add(Long.valueOf(tabletId));
        });
        return beXTablets;
    }

    private String genSQL(Optional<List<String>> columns,
            String schemaName, String tableName,
            TupleDomain<ColumnHandle> predicate,
            int domainLimit)
    {
        List<String> columnsList = ImmutableList.copyOf(columns.orElse(new ArrayList<>()));
        String columnsStr;
        if (!columnsList.isEmpty()) {
            columnsStr = columnsList.stream().map(column -> "`" + column + "`").collect(Collectors.joining(", "));
        }
        else {
            columnsStr = "1";
        }

        String sql = "SELECT " + columnsStr + " FROM " + "`" + schemaName + "`" + "." + "`" + tableName + "`";
        // if the predicate is none, no need to add where clause
        // if the predicate is all, no need to add where clause
        if (!predicate.isNone() && !predicate.isAll()) {
            String whereClause = buildPredicate(predicate, domainLimit);
            if (whereClause != null && !whereClause.isBlank()) {
                sql += " WHERE " + whereClause;
            }
        }
        return sql;
    }

    private String buildPredicate(TupleDomain<ColumnHandle> constraint, int domainLimit)
    {
        if (constraint.isNone() || constraint.isAll()) {
            // no predicate
            return null;
        }

        List<String> conjuncts = new ArrayList<>();

        for (Map.Entry<ColumnHandle, Domain> entry : constraint.getDomains().get().entrySet()) {
            StarrocksColumnHandle columnHandle = (StarrocksColumnHandle) entry.getKey();
//            Domain domain = entry.getValue();
            Domain domain = entry.getValue().simplify(domainLimit);
            String columnName = columnHandle.getColumnName();

            if (domain.isOnlyNull()) {
                conjuncts.add(String.format("`%s` IS NULL", columnName));
            }
            else if (domain.isNullAllowed()) {
                Optional<String> predicate = toPredicate(columnName, domain.getValues(), domain);
                if (predicate.isPresent()) {
                    conjuncts.add(String.format("(`%s` IS NULL OR %s)", columnName, predicate.get()));
                }
                else {
                    conjuncts.add(String.format("`%s` IS NULL", columnName));
                }
            }
            else {
                Optional<String> predicate = toPredicate(columnName, domain.getValues(), domain);
                predicate.ifPresent(conjuncts::add);
            }
        }

        return String.join(" AND ", conjuncts);
    }

    private Optional<String> toPredicate(String columnName, ValueSet valueSet, Domain domain)
    {
        if (valueSet instanceof EquatableValueSet) {
            List<String> values = ((EquatableValueSet) valueSet).getValues().stream()
                    .map(value -> formatLiteral(value, valueSet.getType()))
                    .collect(Collectors.toList());
            if (values.size() == 1) {
                return Optional.of(String.format("`%s` = %s", columnName, values.get(0)));
            }
            return Optional.of(String.format("`%s` IN (%s)", columnName, String.join(", ", values)));
        }
        else if (valueSet instanceof SortedRangeSet) {
            List<Range> ranges = ((SortedRangeSet) valueSet).getOrderedRanges();
            List<String> rangeConjuncts = new ArrayList<>();
            if (valueSet.isAll() && !domain.isNullAllowed()) {
                rangeConjuncts.add(String.format("`%s` IS NOT NULL", columnName));
            }
            if (ranges.stream().allMatch(Range::isSingleValue) && ranges.size() > 1) {
                List<String> values = ranges.stream()
                        .map(value -> formatLiteral(value.getSingleValue(), value.getType()))
                        .collect(toImmutableList());
                String predicate = String.format("`%s` IN (%s)", columnName, String.join(", ", values));
                rangeConjuncts.add(predicate);
            }
            else {
                for (Range range : ranges) {
                    if (range.isSingleValue()) {
                        rangeConjuncts.add(String.format("`%s` = %s", columnName, formatLiteral(range.getSingleValue(), range.getType())));
                    }
                    else {
                        List<String> rangeElements = new ArrayList<>();
                        if (!range.isLowUnbounded()) {
                            String operator = range.isLowInclusive() ? ">=" : ">";
                            rangeElements.add(String.format("`%s` %s %s", columnName, operator, formatLiteral(range.getLowBoundedValue(), range.getType())));
                        }
                        if (!range.isHighUnbounded()) {
                            String operator = range.isHighInclusive() ? "<=" : "<";
                            rangeElements.add(String.format("`%s` %s %s", columnName, operator, formatLiteral(range.getHighBoundedValue(), range.getType())));
                        }
                        if (!rangeElements.isEmpty()) {
                            rangeConjuncts.add(String.join(" AND ", rangeElements));
                        }
                    }
                }
            }

            if (rangeConjuncts.size() == 1) {
                return Optional.of(rangeConjuncts.get(0));
            }
            return Optional.of("(" + String.join(" OR ", rangeConjuncts) + ")");
        }

        throw new IllegalArgumentException("Unsupported ValueSet type: " + valueSet.getClass().getSimpleName());
    }

    public List<String> getSchemaNames(ConnectorSession session)
    {
        String sql = "SHOW DATABASES";
        try (Connection connection = dbClient.openConnection(session);
                Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            List<String> databaseNames = new ArrayList<>();
            while (resultSet.next()) {
                String databaseName = resultSet.getString(1).toLowerCase(Locale.ROOT);
                databaseNames.add(databaseName);
            }
            return databaseNames;
        }
        catch (Exception e) {
            LOG.error("Execute sql {} fail", sql, e);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to list StarRocks schemas: " + e.getMessage(), e);
        }
    }

    public void createSchema(ConnectorSession session, String schemaName)
    {
        executeDdl(session, "CREATE DATABASE " + quotedIdentifier(schemaName));
    }

    public void dropSchema(ConnectorSession session, String schemaName)
    {
        executeDdl(session, "DROP DATABASE " + quotedIdentifier(schemaName));
    }

    public void renameSchema(ConnectorSession session, String source, String target)
    {
        executeDdl(session, "ALTER DATABASE " + quotedIdentifier(source) + " RENAME " + quotedIdentifier(target));
    }

    public List<SchemaTableName> listTables(Optional<String> schemaName, ConnectorSession session)
    {
        try (Connection connection = dbClient.openConnection(session);
                Statement statement = connection.createStatement()) {
            String sql = schemaName
                    .map(schema -> "SELECT TABLE_SCHEMA,TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + schema + "' AND TABLE_TYPE = 'BASE TABLE'")
                    .orElse("SELECT TABLE_SCHEMA,TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'");
            ResultSet resultSet = statement.executeQuery(sql);
            LinkedHashSet<SchemaTableName> relationNames = new LinkedHashSet<>();
            while (resultSet.next()) {
                relationNames.add(new SchemaTableName(
                        resultSet.getString(1),
                        resultSet.getString(2)));
            }
            relationNames.addAll(listViews(schemaName, session));
            relationNames.addAll(listMaterializedViews(schemaName, session));
            return ImmutableList.copyOf(relationNames);
        }
        catch (Exception e) {
            LOG.error("Execute sql fail", e);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to list StarRocks tables: " + e.getMessage(), e);
        }
    }

    public List<SchemaTableName> listMaterializedViews(Optional<String> schemaName, ConnectorSession session)
    {
        try (Connection connection = dbClient.openConnection(session);
                Statement statement = connection.createStatement()) {
            String sql = schemaName
                    .map(schema -> "SELECT TABLE_SCHEMA,TABLE_NAME FROM INFORMATION_SCHEMA.MATERIALIZED_VIEWS WHERE TABLE_SCHEMA = '" + schema + "' AND refresh_type != 'ROLLUP'")
                    .orElse("SELECT TABLE_SCHEMA,TABLE_NAME FROM INFORMATION_SCHEMA.MATERIALIZED_VIEWS WHERE refresh_type != 'ROLLUP'");
            ResultSet resultSet = statement.executeQuery(sql);
            ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
            while (resultSet.next()) {
                tableNames.add(new SchemaTableName(
                        resultSet.getString(1),
                        resultSet.getString(2)));
            }
            return tableNames.build();
        }
        catch (Exception e) {
            LOG.error("Execute sql fail", e);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to list StarRocks materialized views: " + e.getMessage(), e);
        }
    }

    private String cleanStarRocksMaterializedViewDefinition(String originalDefinition)
    {
        Set<String> removeLinePrefix = Set.of("COMMENT", "DISTRIBUTED BY HASH", "REFRESH", "PARTITION BY", "ORDER BY", "PROPERTIES", "CREATE MATERIALIZED VIEW");
        String[] lines = originalDefinition.split("\r?\n|\r");
        StringBuilder result = new StringBuilder();
        boolean insideProperties = false;

        for (String line : lines) {
            String trimmedLine = line.trim();
            if (trimmedLine.isEmpty()) {
                continue;
            }
            if (trimmedLine.toUpperCase().startsWith("PROPERTIES")) {
                insideProperties = true;
                continue;
            }
            if (insideProperties) {
                if (trimmedLine.contains(")")) {
                    insideProperties = false;
                }
                continue;
            }
            boolean shouldKeep = removeLinePrefix.stream()
                    .noneMatch(prefix -> trimmedLine.toUpperCase().startsWith(prefix));

            if (shouldKeep) {
                result.append(line).append(" ");
            }
        }

        String cleanedDefinition = normalizeStoredSql(result.toString());
        if (!cleanedDefinition.isBlank()) {
            return cleanedDefinition;
        }
        return normalizeStoredSql(originalDefinition);
    }

    public List<SchemaTableName> listViews(Optional<String> schemaName, ConnectorSession session)
    {
        try (Connection connection = dbClient.openConnection(session);
                Statement statement = connection.createStatement()) {
            String sql = schemaName
                    .map(schema -> "SELECT TABLE_SCHEMA,TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '" + schema + "'")
                    .orElse("SELECT TABLE_SCHEMA,TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS");
            ResultSet resultSet = statement.executeQuery(sql);
            ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
            while (resultSet.next()) {
                tableNames.add(new SchemaTableName(
                        resultSet.getString(1),
                        resultSet.getString(2)));
            }
            return tableNames.build();
        }
        catch (Exception e) {
            LOG.error("Execute sql fail", e);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to list StarRocks views: " + e.getMessage(), e);
        }
    }

    public boolean viewExists(ConnectorSession session, SchemaTableName viewName)
    {
        return relationExists(
                session,
                "SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                viewName);
    }

    public boolean materializedViewExists(ConnectorSession session, SchemaTableName viewName)
    {
        return relationExists(
                session,
                "SELECT 1 FROM INFORMATION_SCHEMA.MATERIALIZED_VIEWS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND refresh_type != 'ROLLUP'",
                viewName);
    }

    public StarrocksTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        if (tableName == null) {
            return null;
        }
        try (Connection connection = dbClient.openConnection(session)) {
            String tableComment;
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT TABLE_COMMENT, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES " +
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?")) {
                ps.setString(1, tableName.getSchemaName());
                ps.setString(2, tableName.getTableName());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next() || !"BASE TABLE".equalsIgnoreCase(rs.getString("TABLE_TYPE"))) {
                        return null;
                    }
                    tableComment = rs.getString("TABLE_COMMENT");
                }
            }

            String partitionKeysStr = "";
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT PARTITION_KEY FROM information_schema.tables_config " +
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?")) {
                ps.setString(1, tableName.getSchemaName());
                ps.setString(2, tableName.getTableName());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        partitionKeysStr = rs.getString("PARTITION_KEY").replaceAll("`", "");
                    }
                }
            }

            List<String> partitionKeys = List.of(partitionKeysStr.split(", "));
            Map<String, Object> properties = new HashMap<>();
            properties.put("partitioned_by", partitionKeys);

            return new StarrocksTableHandle(
                    tableName,
                    getColumnHandlers(session, tableName, partitionKeys),
                    TupleDomain.all(),
                    Optional.ofNullable(tableComment),
                    Optional.of(partitionKeys),
                    Optional.of(properties));
        }
        catch (SQLException e) {
            LOG.error("Execute sql fail for table {}: {}", tableName, e.getMessage(), e);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to look up table metadata for " + tableName + ": " + e.getMessage(), e);
        }
    }

    private boolean relationExists(ConnectorSession session, String sql, SchemaTableName name)
    {
        try (Connection connection = dbClient.openConnection(session);
                PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, name.getSchemaName());
            preparedStatement.setString(2, name.getTableName());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                return resultSet.next();
            }
        }
        catch (SQLException e) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Failed to determine relation type for " + name + ": " + e.getMessage(),
                    e);
        }
    }

    private Optional<String> getRelationComment(ConnectorSession session, SchemaTableName relationName)
    {
        String sql = """
                SELECT TABLE_COMMENT
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = ?
                  AND TABLE_NAME = ?
                """;
        try (Connection connection = dbClient.openConnection(session);
                PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, relationName.getSchemaName());
            preparedStatement.setString(2, relationName.getTableName());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return Optional.ofNullable(resultSet.getString("TABLE_COMMENT"));
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Failed to retrieve relation comment for " + relationName + ": " + e.getMessage(),
                    e);
        }
    }

    public List<StarrocksColumnHandle> getColumnHandlers(ConnectorSession session, SchemaTableName tableName, List<String> patitionKeys)
    {
        try (Connection connection = dbClient.openConnection(session);
                Statement statement = connection.createStatement()) {
            String sql = "SELECT COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, EXTRA, COLUMN_COMMENT,COLUMN_SIZE,DECIMAL_DIGITS " +
                    "FROM " + INFORMATION_SCHEMA + ".columns " +
                    "WHERE TABLE_SCHEMA = '" + tableName.getSchemaName() +
                    "' AND TABLE_NAME = '" + tableName.getTableName() + "'";
            ResultSet resultSet = statement.executeQuery(sql);
            ImmutableList.Builder<StarrocksColumnHandle> columnMetadata = ImmutableList.builder();
            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");
                int ordinalPosition = resultSet.getInt("ORDINAL_POSITION");
                String dataType = resultSet.getString("DATA_TYPE");
                String columnType = resultSet.getString("COLUMN_TYPE");
                boolean isNullable = resultSet.getBoolean("IS_NULLABLE");
                String extra = patitionKeys.contains(columnName) ? "partition key" : resultSet.getString("EXTRA");
                String comment = resultSet.getString("COLUMN_COMMENT");
                int columnSize = resultSet.getInt("COLUMN_SIZE");
                int decimalDigits = resultSet.getInt("DECIMAL_DIGITS");
                columnMetadata.add(new StarrocksColumnHandle(
                        columnName,
                        ordinalPosition,
                        dataType,
                        columnType,
                        isNullable,
                        extra,
                        comment,
                        columnSize,
                        decimalDigits));
            }
            return columnMetadata.build();
        }
        catch (Exception e) {
            LOG.error("Execute sql fail", e);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to retrieve column metadata: " + e.getMessage(), e);
        }
    }

    public TableStatistics getTableStatistics(ConnectorSession session, StarrocksTableHandle tableHandle)
    {
        String sql = """
                SELECT TABLE_ROWS
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = ?
                  AND TABLE_NAME = ?
                """;

        try (Connection connection = dbClient.openConnection(session);
                PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, tableHandle.getSchemaTableName().getSchemaName());
            preparedStatement.setString(2, tableHandle.getSchemaTableName().getTableName());

            TableStatistics.Builder tableStatistics = TableStatistics.builder();
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    long tableRows = resultSet.getLong("TABLE_ROWS");
                    if (!resultSet.wasNull() && tableRows >= 0) {
                        tableStatistics.setRowCount(Estimate.of(tableRows));
                    }
                }
            }
            return tableStatistics.build();
        }
        catch (SQLException e) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Failed to retrieve table statistics for " + tableHandle.getSchemaTableName() + ": " + e.getMessage(),
                    e);
        }
    }

    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName, StarrocksTypeMapper typeMapper)
    {
        String sql = """
                SELECT VIEW_DEFINITION, DEFINER, SECURITY_TYPE
                FROM INFORMATION_SCHEMA.VIEWS
                WHERE TABLE_SCHEMA = ?
                  AND TABLE_NAME = ?
                """;

        try (Connection connection = dbClient.openConnection(session);
                PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, viewName.getSchemaName());
            preparedStatement.setString(2, viewName.getTableName());

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }

                String originalSql = normalizeStoredSql(resultSet.getString("VIEW_DEFINITION"));
                boolean runAsInvoker = getOptionalString(resultSet, "SECURITY_TYPE")
                        .map(securityType -> securityType.equalsIgnoreCase("INVOKER"))
                        .orElse(false);
                Optional<String> owner = runAsInvoker
                        ? Optional.empty()
                        : Optional.of(getOptionalString(resultSet, "DEFINER")
                                .map(StarrocksFEClient::extractUserFromDefiner)
                                .orElse(config.getUsername()));

                List<ConnectorViewDefinition.ViewColumn> columns = getColumnHandlers(session, viewName, Collections.emptyList()).stream()
                        .map(column -> new ConnectorViewDefinition.ViewColumn(
                                column.getColumnName(),
                                typeMapper.toTrinoType(column.getType(), column.getColumnType(), column.getColumnSize(), column.getDecimalDigits()).getTypeId(),
                                Optional.ofNullable(column.getComment())))
                        .collect(toImmutableList());

                return Optional.of(new ConnectorViewDefinition(
                        originalSql,
                        Optional.of(catalogName),
                        Optional.of(viewName.getSchemaName()),
                        columns,
                        getRelationComment(session, viewName),
                        owner,
                        runAsInvoker,
                        List.of()));
            }
        }
        catch (SQLException e) {
            LOG.error("Execute sql fail", e);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to retrieve view metadata: " + e.getMessage(), e);
        }
    }

    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName, StarrocksTypeMapper typeMapper)
    {
        String sql = """
                SELECT MATERIALIZED_VIEW_DEFINITION
                FROM INFORMATION_SCHEMA.MATERIALIZED_VIEWS
                WHERE TABLE_SCHEMA = ?
                  AND TABLE_NAME = ?
                  AND refresh_type != 'ROLLUP'
                """;

        try (Connection connection = dbClient.openConnection(session);
                PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, viewName.getSchemaName());
            preparedStatement.setString(2, viewName.getTableName());

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }

                String originalSql = cleanStarRocksMaterializedViewDefinition(resultSet.getString("MATERIALIZED_VIEW_DEFINITION"));
                Optional<String> owner = Optional.of(config.getUsername());
                List<ConnectorMaterializedViewDefinition.Column> columns = getColumnHandlers(session, viewName, Collections.emptyList()).stream()
                        .map(column -> new ConnectorMaterializedViewDefinition.Column(
                                column.getColumnName(),
                                typeMapper.toTrinoType(column.getType(), column.getColumnType(), column.getColumnSize(), column.getDecimalDigits()).getTypeId(),
                                Optional.ofNullable(column.getComment())))
                        .collect(toImmutableList());

                return Optional.of(new ConnectorMaterializedViewDefinition(
                        originalSql,
                        Optional.empty(),
                        Optional.of(catalogName),
                        Optional.of(viewName.getSchemaName()),
                        columns,
                        Optional.empty(),
                        getRelationComment(session, viewName),
                        owner,
                        List.of()));
            }
        }
        catch (Exception e) {
            LOG.error("Execute sql fail", e);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to retrieve materialized view metadata: " + e.getMessage(), e);
        }
    }

    public StarrocksQueryInfo getQueryInfo(StarrocksTableHandle tableHandle, TupleDomain<ColumnHandle> predicate, int domainLimit)
            throws IOException
    {
        String[] httpNodes = config.getScanURL().split(",");
        String sql = genSQL(
                Optional.ofNullable(
                tableHandle.getColumns().stream().map(StarrocksColumnHandle::getColumnName).collect(toImmutableList())),
                tableHandle.getSchemaTableName().getSchemaName(),
                tableHandle.getSchemaTableName().getTableName(),
                predicate,
                domainLimit);
        LOG.debug("Generated SQL: {}", sql);
        StarrocksQueryPlan plan = getQueryPlan(
                sql,
                httpNodes[new Random().nextInt(httpNodes.length)],
                config,
                tableHandle);
        Map<String, Set<Long>> beXTablets = transferQueryPlanToBeXTablet(plan);
        List<StarrocksQueryBeXTablets> queryBeXTabletsList = new ArrayList<>();
        beXTablets.forEach((key, value) -> {
            StarrocksQueryBeXTablets queryBeXTablets = new StarrocksQueryBeXTablets(key, new ArrayList<>(value));
            queryBeXTabletsList.add(queryBeXTablets);
        });
        return new StarrocksQueryInfo(plan, queryBeXTabletsList);
    }

    public List<StarrocksSplit> buildStarrocksSplits(StarrocksTableHandle tableHandle, TupleDomain<ColumnHandle> predicate, int domainLimit)
    {
        List<StarrocksSplit> splits = new ArrayList<>();
        try {
            StarrocksQueryInfo queryInfo = getQueryInfo(tableHandle, predicate, domainLimit);
            Map<String, Set<Long>> beXTablets = transferQueryPlanToBeXTablet(queryInfo.getQueryPlan());
            String schemaName = tableHandle.getSchemaTableName().getSchemaName();
            String tableName = tableHandle.getSchemaTableName().getTableName();
            String opaquedQueryPlan = queryInfo.getQueryPlan().getOpaqued_query_plan();
            for (Map.Entry<String, Set<Long>> entry : beXTablets.entrySet()) {
                String beAddress = entry.getKey();
                for (Long tabletId : entry.getValue()) {
                    splits.add(new StarrocksSplit(
                            schemaName,
                            tableName,
                            ImmutableList.of(tabletId),
                            beAddress,
                            opaquedQueryPlan));
                }
            }
        }
        catch (IOException e) {
            LOG.error("Get query info fail", e);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to build StarRocks query splits: " + e.getMessage(), e);
        }
        return splits;
    }

    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition)
    {
        String sql = "CREATE VIEW " + quotedTable(viewName) + " AS " + definition.getOriginalSql();
        executeDdl(session, sql);
    }

    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        executeDdl(session, "DROP VIEW " + quotedTable(viewName));
    }

    public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition)
    {
        String sql = "CREATE MATERIALIZED VIEW " + quotedTable(viewName) + " AS " + definition.getOriginalSql();
        executeDdl(session, sql);
    }

    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        executeDdl(session, "DROP MATERIALIZED VIEW " + quotedTable(viewName));
    }

    public void refreshMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        executeDdl(session, "REFRESH MATERIALIZED VIEW " + quotedTable(viewName));
    }

    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode, StarrocksTypeMapper typeMapper)
    {
        List<?> partitionedBy = (List<?>) tableMetadata.getProperties().getOrDefault(StarrocksTableProperties.PROPERTIES_PARTITIONED_BY, List.of());
        if (!partitionedBy.isEmpty()) {
            throw new UnsupportedOperationException("partitioned_by table property is not supported for CREATE TABLE");
        }
        if (tableMetadata.getColumns().isEmpty()) {
            throw new UnsupportedOperationException("StarRocks table must have at least one column");
        }

        String columnsSql = tableMetadata.getColumns().stream()
                .map(column -> getColumnDefinition(column, typeMapper))
                .collect(Collectors.joining(", "));
        Optional<String> keyColumn = tableMetadata.getColumns().stream()
                .filter(column -> isKeyColumnType(column.getType()))
                .map(ColumnMetadata::getName)
                .findFirst();

        StringBuilder sql = new StringBuilder("CREATE TABLE ");
        if (saveMode == SaveMode.IGNORE) {
            sql.append("IF NOT EXISTS ");
        }
        sql.append(quotedTable(tableMetadata.getTable()))
                .append(" (")
                .append(columnsSql)
                .append(")");

        tableMetadata.getComment().ifPresent(comment -> sql.append(" COMMENT ").append(quotedString(comment)));
        keyColumn.ifPresent(column -> sql.append(" DUPLICATE KEY(").append(quotedIdentifier(column)).append(")"));
        if (keyColumn.isPresent()) {
            sql.append(" DISTRIBUTED BY HASH(").append(quotedIdentifier(keyColumn.orElseThrow())).append(") BUCKETS ").append(DEFAULT_BUCKETS);
        }
        else {
            sql.append(" DISTRIBUTED BY RANDOM BUCKETS ").append(DEFAULT_BUCKETS);
        }

        executeDdl(session, sql.toString());
    }

    public void dropTable(ConnectorSession session, SchemaTableName tableName)
    {
        executeDdl(session, "DROP TABLE " + quotedTable(tableName));
    }

    public void dropTableIfExists(ConnectorSession session, SchemaTableName tableName)
    {
        executeDdl(session, "DROP TABLE IF EXISTS " + quotedTable(tableName));
    }

    public void renameTable(ConnectorSession session, SchemaTableName tableName, SchemaTableName newTableName)
    {
        if (!tableName.getSchemaName().equals(newTableName.getSchemaName())) {
            throw new UnsupportedOperationException("Renaming tables across schemas is not supported");
        }
        String sql = "ALTER TABLE " + quotedTable(tableName) + " RENAME " + quotedIdentifier(newTableName.getTableName());
        executeDdl(session, sql);
    }

    public void addColumn(ConnectorSession session, SchemaTableName tableName, ColumnMetadata column, StarrocksTypeMapper typeMapper)
    {
        String sql = "ALTER TABLE " + quotedTable(tableName) + " ADD COLUMN " + getColumnDefinition(column, typeMapper);
        executeDdl(session, sql);
    }

    public void renameColumn(ConnectorSession session, SchemaTableName tableName, String source, String target)
    {
        String sql = "ALTER TABLE " + quotedTable(tableName) + " RENAME COLUMN " + quotedIdentifier(source) + " " + quotedIdentifier(target);
        executeDdl(session, sql);
    }

    public void dropColumn(ConnectorSession session, SchemaTableName tableName, String columnName)
    {
        String sql = "ALTER TABLE " + quotedTable(tableName) + " DROP COLUMN " + quotedIdentifier(columnName);
        executeDdl(session, sql);
    }

    public void setColumnType(ConnectorSession session, SchemaTableName tableName, StarrocksColumnHandle column, Type type, StarrocksTypeMapper typeMapper)
    {
        StringBuilder sql = new StringBuilder("ALTER TABLE ")
                .append(quotedTable(tableName))
                .append(" MODIFY COLUMN ")
                .append(quotedIdentifier(column.getColumnName()))
                .append(" ")
                .append(typeMapper.toStarrocksType(type));
        if (!column.isNullable()) {
            sql.append(" NOT NULL");
        }
        if (column.getComment() != null && !column.getComment().isBlank()) {
            sql.append(" COMMENT ").append(quotedString(column.getComment()));
        }
        executeDdl(session, sql.toString());
    }

    private String getColumnDefinition(ColumnMetadata column, StarrocksTypeMapper typeMapper)
    {
        StringBuilder definition = new StringBuilder()
                .append(quotedIdentifier(column.getName()))
                .append(" ")
                .append(typeMapper.toStarrocksType(column.getType()));
        if (!column.isNullable()) {
            definition.append(" NOT NULL");
        }
        if (column.getComment() != null && !column.getComment().isBlank()) {
            definition.append(" COMMENT ").append(quotedString(column.getComment()));
        }
        return definition.toString();
    }

    private boolean isKeyColumnType(Type type)
    {
        String baseType = type.getBaseName();
        return !baseType.equals(StandardTypes.ARRAY) &&
                !baseType.equals(StandardTypes.MAP) &&
                !baseType.equals(StandardTypes.ROW) &&
                !baseType.equals(StandardTypes.JSON) &&
                !baseType.equals(StandardTypes.VARBINARY);
    }

    private void executeDdl(ConnectorSession session, String sql)
    {
        try (Connection connection = dbClient.openConnection(session);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            LOG.error("Execute DDL failed: {}", sql, e);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "DDL execution failed: " + e.getMessage(), e);
        }
    }

    private static String quotedTable(SchemaTableName tableName)
    {
        return quotedIdentifier(tableName.getSchemaName()) + "." + quotedIdentifier(tableName.getTableName());
    }

    private static String quotedIdentifier(String identifier)
    {
        return "`" + identifier.replace("`", "``") + "`";
    }

    private static String quotedString(String value)
    {
        return "'" + value.replace("'", "''") + "'";
    }

    private static String normalizeStoredSql(String sql)
    {
        if (sql == null) {
            return "";
        }
        String normalized = sql.replace("`", "\"").trim();
        if (normalized.endsWith(";")) {
            normalized = normalized.substring(0, normalized.length() - 1).trim();
        }
        return normalized;
    }

    private static Optional<String> getOptionalString(ResultSet resultSet, String columnName)
            throws SQLException
    {
        ResultSetMetaData metadata = resultSet.getMetaData();
        for (int columnIndex = 1; columnIndex <= metadata.getColumnCount(); columnIndex++) {
            if (columnName.equalsIgnoreCase(metadata.getColumnLabel(columnIndex)) ||
                    columnName.equalsIgnoreCase(metadata.getColumnName(columnIndex))) {
                return Optional.ofNullable(resultSet.getString(columnIndex));
            }
        }
        return Optional.empty();
    }

    private static String extractUserFromDefiner(String definer)
    {
        String user = definer.split("@", 2)[0];
        return user.replace("`", "").replace("'", "");
    }

    private String formatLiteral(Object value, Type type)
    {
        value = type.getObjectValue(null, nativeValueToBlock(type, value), 0);
        if (value == null) {
            return "NULL";
        }
        if (type instanceof VarcharType) {
            return "'" + value.toString().replace("'", "''") + "'";
        }
        else if (type instanceof BigintType || type instanceof IntegerType || type instanceof SmallintType || type instanceof TinyintType) {
            return value.toString();
        }
        else if (type instanceof DoubleType || type instanceof RealType || type instanceof DecimalType) {
            return new BigDecimal(value.toString()).toPlainString();
        }
        else if (type instanceof BooleanType) {
            return ((Boolean) value) ? "1" : "0";
        }
        else if (type instanceof DateType) {
            return "'" + value + "'";
        }
        else if (type instanceof TimestampType) {
            return "'" + value + "'";
        }

        throw new UnsupportedOperationException("Unsupported literal type: " + type.getDisplayName());
    }

    public OptionalLong executeUpdate(ConnectorSession session, StarrocksUpdateTableHandle handle, int domainLimit)
    {
        StarrocksTableHandle tableHandle = handle.getTableHandle();
        if (tableHandle.getConstraint().isNone()) {
            return OptionalLong.of(0);
        }

        String assignments = handle.getAssignments().entrySet().stream()
                .map(entry -> "`" + entry.getKey() + "` = " + entry.getValue())
                .collect(Collectors.joining(", "));
        if (assignments.isBlank()) {
            return OptionalLong.of(0);
        }

        String sql = "UPDATE `" + tableHandle.getSchemaTableName().getSchemaName() + "`.`" + tableHandle.getSchemaTableName().getTableName() + "` SET " + assignments;
        if (!tableHandle.getConstraint().isAll()) {
            String whereClause = buildPredicate(tableHandle.getConstraint(), domainLimit);
            if (whereClause != null && !whereClause.isBlank()) {
                sql = sql + " WHERE " + whereClause;
            }
        }

        try (Connection connection = dbClient.openConnection(session);
                Statement statement = connection.createStatement()) {
            return OptionalLong.of(statement.executeUpdate(sql));
        }
        catch (SQLException e) {
            LOG.error("Execute update SQL failed: {}", sql, e);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "UPDATE execution failed: " + e.getMessage(), e);
        }
    }

    public MaterializedViewFreshness getMaterializedViewFreshness(SchemaTableName name, ConnectorSession session)
    {
        String sql = """
        SELECT LAST_REFRESH_FINISHED_TIME 
        FROM INFORMATION_SCHEMA.MATERIALIZED_VIEWS 
        WHERE TABLE_SCHEMA = ? 
        AND TABLE_NAME = ? 
        AND refresh_type != 'ROLLUP'
        """;

        try (Connection connection = dbClient.openConnection(session);
                PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setString(1, name.getSchemaName());
            preparedStatement.setString(2, name.getTableName());

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    long lastRefreshTime = resultSet.getLong("LAST_REFRESH_FINISHED_TIME");
                    return new MaterializedViewFreshness(
                            MaterializedViewFreshness.Freshness.UNKNOWN,
                            Optional.ofNullable(Instant.ofEpochSecond(lastRefreshTime))
                    );
                }
                return new MaterializedViewFreshness(
                        MaterializedViewFreshness.Freshness.UNKNOWN,
                        Optional.empty()
                );
            }
        } catch (SQLException e) {
            LOG.error("Failed to get materialized view freshness for {}: {}", name, e.getMessage(), e);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to get materialized view freshness for " + name + ": " + e.getMessage(), e);
        }
    }
}
