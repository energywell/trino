# StarRocks Connector for Trino

A Trino connector for [StarRocks](https://www.starrocks.io/) that reads data directly from BE (backend) nodes via the Arrow-based scan API and executes aggregate and join queries via a JDBC connection to the FE (frontend).

---

## Contents

- [Architecture](#architecture)
- [Configuration](#configuration)
- [Session Properties](#session-properties)
- [Type Mapping](#type-mapping)
- [Query Push-downs](#query-push-downs)
- [Split Strategy](#split-strategy)
- [Schema and Table Management](#schema-and-table-management)
- [Views and Materialized Views](#views-and-materialized-views)
- [Write Support](#write-support)
- [Resilience](#resilience)
- [Developer Runbook](#developer-runbook)

---

## Architecture

The connector uses two data paths depending on the query shape.

**Regular scans** — The FE is queried for a tablet routing plan over HTTP. Trino creates one split per tablet batch, and each split streams rows directly from the responsible BE node using the Arrow-based scan API. Predicates, column projections, and LIMIT are applied at the BE level to minimise transferred data.

**Aggregate and join queries** — When aggregations or joins are pushed down, the connector generates a single SQL statement and executes it over JDBC against the FE. The FE runs the query natively and returns results, which are read back via a JDBC `ResultSet`.

---

## Configuration

Set these properties in `etc/catalog/<catalog-name>.properties`.

### Connection

| Property | Required | Default | Description |
|---|---|---|---|
| `connector.name` | yes | — | Must be `starrocks` |
| `jdbc-url` | yes | — | JDBC URL for the StarRocks FE. Example: `jdbc:mysql://host:9030` |
| `scan-url` | yes | — | HTTP endpoint(s) for StarRocks BE nodes. Example: `host:8030`. Comma-separated for multiple nodes. |
| `username` | yes | — | StarRocks user |
| `password` | no | _(empty)_ | Password for the StarRocks user |

### Timeouts

| Property | Default | Min | Description |
|---|---|---|---|
| `scan-connect-timeout` | `1s` | `1ms` | Timeout for establishing FE/BE network connections |
| `scan-read-timeout` | `30s` | `1ms` | Timeout for HTTP reads from FE and BE |
| `scan-write-timeout` | `30s` | `1ms` | Timeout for HTTP writes (stream load) |
| `scan-query-timeout` | `10m` | `1ms` | Timeout covering the FE query plan request and the BE scan |
| `scan-keep-alive` | `1m` | `1ms` | Keep-alive duration for the BE scanner connection |

### Scan Behaviour

| Property | Default | Min | Description |
|---|---|---|---|
| `scan-batch-rows` | `1000` | `1` | Row batch size for the BE Arrow scanner and for stream-load write buffering |
| `scan-tablets-per-split` | `16` | `1` | Maximum tablets grouped into one Trino split. Lower values increase read parallelism; higher values reduce split scheduling overhead. |
| `scan-max-retries` | `3` | `1` | Maximum retry attempts for FE query plan requests |

### Dynamic Filtering and Predicates

| Property | Default | Min | Description |
|---|---|---|---|
| `dynamic-filtering-wait-timeout` | `10s` | `0ms` | How long to wait for dynamic filters before generating splits. Set to `0ms` to disable waiting. |
| `tuple-domain-limit` | `1000` | `1` | Maximum number of discrete values in a pushed-down IN-list predicate. Predicates exceeding this limit are not pushed to StarRocks. |

### Example

```properties
connector.name=starrocks
jdbc-url=jdbc:mysql://starrocks-fe:9030
scan-url=starrocks-be-1:8030,starrocks-be-2:8030
username=admin
password=secret
scan-tablets-per-split=8
scan-query-timeout=5m
dynamic-filtering-wait-timeout=15s
```

---

## Session Properties

Session properties override the corresponding catalog-level config for a single query session.

| Property | Type | Default | Description |
|---|---|---|---|
| `dynamic_filtering_wait_timeout` | duration | from config | Dynamic filter wait timeout |
| `tuple_domain_limit` | integer | from config | IN-list predicate size limit |

```sql
SET SESSION starrocks.tuple_domain_limit = 500;
SET SESSION starrocks.dynamic_filtering_wait_timeout = '30s';
```

---

## Type Mapping

### StarRocks → Trino

| StarRocks Type | Trino Type | Notes |
|---|---|---|
| `boolean` | `BOOLEAN` | |
| `tinyint` | `TINYINT` | `tinyint(1)` maps to `BOOLEAN` |
| `smallint` | `SMALLINT` | |
| `int` / `integer` | `INTEGER` | |
| `bigint` | `BIGINT` | |
| `bigint unsigned` | `DECIMAL(38, 0)` | No unsigned integer type in Trino |
| `largeint` | `DECIMAL(38, 0)` | StarRocks 128-bit integer |
| `float` | `REAL` | |
| `double` | `DOUBLE` | |
| `decimal(p, s)` | `DECIMAL(p, s)` | Precision and scale preserved |
| `decimal32` / `decimal64` / `decimal128` / `decimalv2` | `DECIMAL` | |
| `char(n)` | `VARCHAR` | |
| `varchar(n)` | `VARCHAR(n)` | Values wider than 65,533 chars use unbounded `VARCHAR` |
| `string` | `VARCHAR` | Unbounded |
| `varbinary` | `VARBINARY` | |
| `date` | `DATE` | |
| `datetime` | `TIMESTAMP(3)` | Millisecond precision |
| `json` | `JSON` | |
| `array<T>` | `ARRAY(Trino(T))` | Recursive element mapping |
| `map<K, V>` | `MAP(Trino(K), Trino(V))` | Recursive key/value mapping |
| `struct<f1 T1, f2 T2, ...>` | `ROW(f1 Trino(T1), f2 Trino(T2), ...)` | Field names preserved |

### Trino → StarRocks

| Trino Type | StarRocks Type |
|---|---|
| `BOOLEAN` | `boolean` |
| `TINYINT` | `tinyint` |
| `SMALLINT` | `smallint` |
| `INTEGER` | `int` |
| `BIGINT` | `bigint` |
| `REAL` | `float` |
| `DOUBLE` | `double` |
| `DECIMAL(p, s)` | `decimal(p, s)` |
| `CHAR(n)` | `char(n)` |
| `VARCHAR(n)` | `varchar(n)` (or `string` if n > 65,533) |
| `VARBINARY` | `varbinary` |
| `DATE` | `date` |
| `TIMESTAMP` | `datetime` |
| `JSON` | `json` |
| `ARRAY(T)` | `array<StarRocks(T)>` |
| `MAP(K, V)` | `map<StarRocks(K), StarRocks(V)>` |
| `ROW(...)` | `struct<field StarRocks(T), ...>` |

---

## Query Push-downs

The connector implements a full stack of push-downs. When a push-down applies, the work is done inside StarRocks rather than Trino, reducing data transfer and coordinator CPU usage.

### Projection push-down

Only the columns referenced by the query are requested from StarRocks. Unreferenced columns are never transferred.

### Predicate push-down

Column predicates derived from `WHERE` clauses are pushed to the StarRocks scan. For regular scans these become tablet-level filters at the BE; for aggregate and join queries they become `WHERE` clauses in the generated SQL.

| Shape | SQL example |
|---|---|
| Equality | `col = value` |
| Inequality | `col <> value` |
| Range | `col >= x AND col <= y` |
| Open range | `col > x`, `col < y` |
| IN list | `col IN (a, b, c)` |
| IS NULL | `col IS NULL` |
| IS NOT NULL | `col IS NOT NULL` |
| NULL-inclusive range | `col IS NULL OR col >= x` |

IN-list predicates exceeding `tuple-domain-limit` are not pushed and are evaluated by Trino instead.

### Expression push-down

Compound filter conditions (LIKE, cross-column OR, etc.) are translated to SQL and pushed alongside the column predicates.

| Trino operator | SQL |
|---|---|
| `$equal` | `=` |
| `$not_equal` | `<>` |
| `$less_than` | `<` |
| `$less_than_or_equal` | `<=` |
| `$greater_than` | `>` |
| `$greater_than_or_equal` | `>=` |
| `$like` | `LIKE` |
| `$is_null` | `IS NULL` |
| `$and` | `AND` |
| `$or` | `OR` |
| `$not` | `NOT` |

Expressions that cannot be fully translated are left in Trino (partial push-down). When an expression is fully translated it is embedded into the per-side subquery for join queries and into the `WHERE` clause for aggregate queries; the residual expression returned to Trino is `TRUE`.

### Limit push-down

`LIMIT n` is pushed to StarRocks. For regular scans the limit is applied per split at the BE level, capping how many rows each split returns before Trino applies the final global limit.

### TopN push-down (ORDER BY … LIMIT)

When a query contains `ORDER BY` and `LIMIT`, the combined top-N is pushed to StarRocks.

**NULL ordering note:** StarRocks sorts NULLs as smaller than all non-NULL values (NULLs first on ASC, last on DESC). Trino's default is ASC NULLS LAST / DESC NULLS FIRST. To avoid wrong results from per-split pre-limiting with mismatched NULL placement, the per-split row cap is suppressed when a sort order is present — each split returns all its rows, and Trino's TopN operator handles the global sort and final limit correctly.

### Aggregate push-down

The following aggregate functions are pushed to StarRocks and executed as a single JDBC query:

| Function | DISTINCT supported |
|---|---|
| `COUNT(*)` | — |
| `COUNT(col)` | yes |
| `SUM(col)` | yes |
| `MIN(col)` | no |
| `MAX(col)` | no |
| `AVG(col)` | no |

**Constraints:**
- Single grouping set only. `GROUPING SETS`, `ROLLUP`, and `CUBE` are not pushed.
- Aggregate functions with a `FILTER (WHERE ...)` clause or an inline `ORDER BY` are not pushed.
- Aggregation is not pushed when the handle already carries a join push-down.

Post-aggregation filters (`HAVING`) are pushed as a `HAVING` clause in the generated SQL.

Generated SQL shape:

```sql
SELECT col1, col2,
       COUNT(*) AS `_agg_0`,
       SUM(amount) AS `_agg_1`
FROM `schema`.`table`
WHERE col1 >= 10
GROUP BY col1, col2
HAVING `_agg_0` > 100
LIMIT 1000
```

### Join push-down

Two-table joins where both sides are StarRocks tables in the same catalog are pushed as a single JDBC query to the FE.

**Supported join types:** `INNER JOIN`, `LEFT OUTER JOIN`, `RIGHT OUTER JOIN`, `FULL OUTER JOIN`

**Supported join conditions:** Any translatable expression using the operators listed in the expression push-down section.

**Predicate placement — outer join correctness:** Each side's predicates (both column and expression filters) are applied inside per-side subqueries rather than in a flat outer `WHERE` clause. A predicate in an outer `WHERE` would silently filter null-extended rows and convert any `OUTER JOIN` to an `INNER JOIN`. The subquery structure prevents this.

**Constraints:**
- Nested join push-down is not supported (a join on top of an already-pushed join).
- Joins where either side carries an aggregation push-down are not pushed.
- The join condition must be fully translatable; untranslatable conditions fall back to Trino.

Generated SQL shape:

```sql
SELECT _l.`col1` AS `_lc_0`, _l.`col2` AS `_lc_1`,
       _r.`col3` AS `_rc_0`
FROM   (SELECT * FROM `schema`.`left_table`
        WHERE col1 = 5 AND (`name` LIKE 'Alice%')) AS _l
LEFT OUTER JOIN
       (SELECT * FROM `schema`.`right_table`
        WHERE col3 < 100) AS _r
ON _l.`id` = _r.`left_id`
```

Output columns are aliased as `_lc_0, _lc_1, ...` (left) and `_rc_0, _rc_1, ...` (right) and mapped back to the original Trino column handles.

### Dynamic filter integration

The split manager waits up to `dynamic-filtering-wait-timeout` for broadcast hash join dynamic filters to become available before generating splits. Dynamic filter predicates are merged with the table's static constraints and pushed to the BE scanner, allowing entire tablets to be skipped before any data is transferred.

---

## Split Strategy

### Regular table scans

1. The connector sends the column list and predicate to the FE via `/api/schema/table/_query_plan`.
2. The FE returns a map of `BE address → tablet list`.
3. Tablets are grouped per BE and batched by `scan-tablets-per-split`.
4. Each Trino split carries: a BE address, a list of tablet IDs, and the opaque query plan blob from the FE.

Example with `scan-tablets-per-split=2` and 5 tablets on BE-1:

| Split | BE | Tablets |
|---|---|---|
| 1 | BE-1 | [1, 2] |
| 2 | BE-1 | [3, 4] |
| 3 | BE-1 | [5] |

### Aggregate and join queries

A single `StarrocksAggregateSplit` is generated containing the complete SQL statement. All computation happens in StarRocks via one JDBC execution. Outer `WHERE`, `ORDER BY`, and `LIMIT` from the handle state are appended to the join subquery wrapper when present.

---

## Schema and Table Management

| Operation | Supported |
|---|---|
| `SHOW SCHEMAS` | yes |
| `CREATE SCHEMA` | yes |
| `DROP SCHEMA` | yes (must be empty) |
| `RENAME SCHEMA` | yes |
| `SHOW TABLES` | yes |
| `CREATE TABLE` | yes |
| `CREATE TABLE AS SELECT` | yes |
| `DROP TABLE` | yes |
| `RENAME TABLE` | yes |
| `ADD COLUMN` | yes |
| `DROP COLUMN` | yes |
| `RENAME COLUMN` | yes |
| `ALTER COLUMN TYPE` | yes |
| Table row-count statistics | yes (via `INFORMATION_SCHEMA.TABLES`) |

The following built-in StarRocks schemas are hidden from schema and table listings:
`information_schema`, `_statistics_`, `sys`

---

## Views and Materialized Views

| Operation | Supported |
|---|---|
| `CREATE VIEW` | yes |
| `DROP VIEW` | yes |
| `RENAME VIEW` | yes |
| `CREATE MATERIALIZED VIEW` | yes |
| `DROP MATERIALIZED VIEW` | yes |
| `REFRESH MATERIALIZED VIEW` | yes |
| Materialized view freshness | yes |

Materialized view freshness is reported using the StarRocks `LAST_REFRESH_FINISHED_TIME` field. The state is `FRESH` when a refresh timestamp is available and `UNKNOWN` otherwise.

---

## Write Support

### INSERT (Stream Load)

Rows written via `INSERT INTO` are sent to StarRocks using the HTTP Stream Load transaction API:

1. `POST /api/transaction/begin` — opens a transaction
2. `POST /api/transaction/load` — streams row data in batches (batch size = `scan-batch-rows`)
3. `POST /api/transaction/prepare` — prepares the transaction
4. `POST /api/transaction/commit` — commits
5. `POST /api/transaction/rollback` — called on any failure, including commit failure

Write memory is bounded by `scan-batch-rows`. Batches are flushed and sent before accumulating more rows.

### UPDATE

`UPDATE … WHERE` is pushed to StarRocks over JDBC when the predicate is translatable.

---

## Resilience

### BE endpoint failover

When `scan-url` lists multiple BE endpoints, the connector tracks failures per endpoint. Failed endpoints receive a temporary penalty with jittered exponential backoff (up to 30 seconds). Healthy endpoints are selected at random. This allows the connector to continue operating when one or more BE nodes are temporarily unavailable.

### FE query plan retries

FE query plan HTTP requests are retried up to `scan-max-retries` times (default: 3) with backoff before the query is failed.

### JDBC connection cleanup

JDBC connections for aggregate and join queries are explicitly lifecycle-managed:
- If a connection is opened but query execution fails during initialisation, the connection is closed before the exception propagates.
- If a `Statement` is created but `executeQuery` fails, the statement is closed before the exception propagates.

---

## Developer Runbook

The stable local server lives at `~/trino-server-480-SNAPSHOT/`. Config (including `starrocks.properties`) lives there and is never overwritten by plugin rebuilds. Connect from DBeaver using the Trino JDBC driver at `localhost:8080`.

### Config-only change (no code changed)

```bash
nano ~/trino-server-480-SNAPSHOT/etc/catalog/starrocks.properties
cd ~/trino-server-480-SNAPSHOT && bin/launcher stop && bin/launcher start
```

### Connector code changed

```bash
cd /Users/stevenchung/Documents/Work/energywell/trino

# 1. Test and install the plugin into the local Maven repository
./mvnw -pl plugin/trino-starrocks test -DskipITs -DfailIfNoTests=false
./mvnw -pl plugin/trino-starrocks -DskipTests install

# 2. Rebuild the server distribution
./mvnw -pl core/trino-server -am -DskipTests clean package

# 3. Copy new plugins into the stable server (etc/ is not touched)
cp -r core/trino-server/target/trino-server-480-SNAPSHOT/plugin ~/trino-server-480-SNAPSHOT/

# 4. Restart
cd ~/trino-server-480-SNAPSHOT && bin/launcher stop && bin/launcher start

# 5. Verify
curl -s http://localhost:8080/v1/info
```

> Use `install` in step 1, not `package`. `package` builds the jar locally but does not put it in the local Maven repository, so the server rebuild in step 2 would pick up the older snapshot instead.
