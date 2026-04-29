# StarRocks connector

The StarRocks connector allows querying tables and views in an external
[StarRocks](https://www.starrocks.io/) cluster.

This connector is purpose-built for StarRocks. It does not rely on the generic
MySQL connector path, because StarRocks metadata and type reporting do not
always match MySQL connector assumptions.

In v1, the connector is read-only:

- metadata is discovered through the native StarRocks JDBC driver
- table reads are executed through Arrow Flight SQL
- writes and schema changes are intentionally out of scope

## Requirements

To connect to StarRocks, you need:

- StarRocks 3.5.1 or higher
- network access from the Trino coordinator and workers to the StarRocks FE
  JDBC endpoint and Arrow Flight SQL endpoint
- a StarRocks user with permission to inspect metadata and run read queries

Arrow Flight SQL must be enabled on both FE and BE nodes, and the FE should be
reachable by Trino on the configured Flight SQL port.

If Trino cannot reach BE nodes directly, configure StarRocks FE proxy mode for
new Arrow Flight SQL sessions using a routable FE endpoint, for example:

```sql
SET GLOBAL arrow_flight_proxy_enabled = true;
SET GLOBAL arrow_flight_proxy = 'fe1.example.net:9408';
```

Some Arrow Flight SQL deployments also require JVM `--add-opens` flags on Java
9 or later. If StarRocks or the Flight SQL client reports Arrow access errors,
apply the `java.nio` opens recommended by the StarRocks Arrow Flight SQL
documentation.

## Connector architecture

The connector uses two StarRocks-native interfaces:

- StarRocks JDBC from `starrocks.jdbc-url` for schema, table, view, and column
  discovery
- StarRocks Arrow Flight SQL from `starrocks.flight-sql-host` and
  `starrocks.flight-sql-port` for data reads

This split is intentional:

- JDBC is used as the source of truth for metadata because it is more reliable
  than MySQL-emulation metadata paths for StarRocks
- Arrow Flight SQL is used for reads because it is the best fit for
  high-throughput, columnar result transport

## Configuration

To configure the StarRocks connector, create a catalog properties file in
`etc/catalog`, for example `example.properties`, with the following contents:

```text
connector.name=starrocks
starrocks.jdbc-url=jdbc:starrocks://fe1.example.net:9030
starrocks.username=trino
starrocks.password=secret
starrocks.flight-sql-host=fe1.example.net
starrocks.flight-sql-port=9408
```

The configuration properties are:

:::{list-table} StarRocks configuration properties
:widths: 30, 20, 50
:header-rows: 1

* - Property name
  - Required
  - Description
* - `starrocks.jdbc-url`
  - Yes
  - StarRocks JDBC URL used for metadata discovery.
* - `starrocks.catalog-name`
  - No
  - StarRocks catalog to expose through this Trino catalog. When set, Trino
    schemas map to databases inside the configured StarRocks catalog.
* - `starrocks.username`
  - No
  - StarRocks username used for JDBC metadata access and Flight SQL sessions.
* - `starrocks.password`
  - No
  - StarRocks password used with `starrocks.username`.
* - `starrocks.flight-sql-host`
  - No
  - Explicit FE host for Arrow Flight SQL. If omitted, the connector derives
    the host from `starrocks.jdbc-url`.
* - `starrocks.flight-sql-port`
  - No
  - FE Arrow Flight SQL port used for reads. The default is `9408`.
:::

If you create multiple catalog properties files, Trino creates one StarRocks
catalog for each file. The catalog name is the filename without the
`.properties` suffix.

## Metadata behavior

The StarRocks connector exposes StarRocks metadata through Trino's lowercase
identifier model.

- Each StarRocks database appears as a Trino schema.
- If `starrocks.catalog-name` is configured, each database in that StarRocks
  catalog appears as a Trino schema.
- `SHOW SCHEMAS` excludes internal schemas such as `information_schema`,
  `_statistics_`, and `sys`.
- `SHOW TABLES` lists readable base tables and views.
- Mixed-case StarRocks schema, table, and column names are resolved back to
  the remote names during reads, as long as the lowercase name is unique.

The connector uses StarRocks-native metadata paths and prefers
`INFORMATION_SCHEMA` for column details when it exposes more accurate type
information than JDBC metadata alone. This avoids breaking metadata access for
tables that fail under the generic MySQL path or that expose StarRocks-specific
types through lossy JDBC metadata.

## Type mapping

The connector maps StarRocks types to Trino types conservatively.

### StarRocks to Trino type mapping

:::{list-table} StarRocks to Trino type mapping
:widths: 30, 30, 40
:header-rows: 1

* - StarRocks type
  - Trino type
  - Notes
* - `BOOLEAN`, `BOOL`
  - `BOOLEAN`
  -
* - `TINYINT`
  - `BOOLEAN` or `TINYINT`
  - `TINYINT(1)` and boolean aliases map to `BOOLEAN`.
* - `SMALLINT`
  - `SMALLINT`
  -
* - `INT`, `INTEGER`
  - `INTEGER`
  -
* - `BIGINT`
  - `BIGINT`
  -
* - `DECIMAL`, `DECIMALV2`, `DECIMALV3`, `DECIMAL32`, `DECIMAL64`,
    `DECIMAL128`, `DECIMAL128I`, `DECIMAL256`
  - `DECIMAL(p, s)` or `VARCHAR`
  - Maps to `VARCHAR` when StarRocks precision is missing or does not fit in
    Trino `DECIMAL`.
* - `FLOAT`
  - `REAL`
  -
* - `DOUBLE`
  - `DOUBLE`
  -
* - `CHAR(n)`
  - `CHAR(n)`
  -
* - `VARCHAR(n)`
  - `VARCHAR(n)`
  -
* - `DATE`
  - `DATE`
  -
* - `DATETIME`, `DATETIMEV2`, `TIMESTAMP`
  - `TIMESTAMP(p)`
  - Precision is derived from StarRocks type metadata when available.
* - `BINARY`, `VARBINARY`
  - `VARBINARY`
  -
* - `JSON`, `VARIANT`
  - `JSON`
  - Values are read through the Arrow Flight SQL path as JSON text.
* - `STRING`, `TEXT`
  - `VARCHAR`
  - These values are read textually in v1.
* - `LARGEINT`, `BITMAP`, `HLL`, `PERCENTILE`, `ARRAY`, `MAP`, `STRUCT`
  - `VARCHAR`
  - These types are preserved for metadata visibility and mapped
    conservatively in v1.
:::

## Querying StarRocks

You can inspect StarRocks metadata with standard Trino commands:

```sql
SHOW SCHEMAS FROM example;
SHOW TABLES FROM example.analytics;
DESCRIBE example.analytics.events;
SHOW CREATE TABLE example.analytics.events;
```

You can query StarRocks tables and views with standard `SELECT` statements:

```sql
SELECT count(*) FROM example.analytics.events;
SELECT id, created_at, name FROM example.analytics.events ORDER BY id;
SELECT * FROM example.analytics.events_view;
```

The connector pushes down basic predicates, `LIMIT`, `ORDER BY ... LIMIT`, and
`count` aggregations into the SQL submitted through Arrow Flight SQL.

## Limitations

The initial version of the connector intentionally does not support:

- `INSERT`, `UPDATE`, `DELETE`, or `MERGE`
- table creation, schema creation, or other write-path DDL
- native Trino `ARRAY`, `MAP`, or `ROW` decoding for StarRocks complex types
- aggregations other than `count`
- advanced split planning beyond the FE Arrow Flight SQL path

The connector prioritizes correct metadata discovery and stable read behavior
for StarRocks-native deployments.
