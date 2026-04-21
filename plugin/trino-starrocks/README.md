# Trino StarRocks Connector: Post-Remediation Production Readiness Reassessment

## Verdict

Current status: **production-ready for staged rollout**.

The previous P0-A and P1-A blockers were implemented:

- write path now uses explicit Stream Load transactions with commit/rollback,
- write buffering is bounded and chunked,
- failure-path transaction behavior is covered by targeted tests.

Module checks and tests are green.

## What was reassessed

- Connector lifecycle and DI wiring
- Configuration validation and operational controls
- FE/BE/StreamLoad timeout and error handling
- Read/write path behavior
- Handle correctness and metadata null-safety
- Unit test coverage and build status

## Validation snapshot

- Local static checks: pass (no IDE errors)
- Module tests: pass
- Command used:

```bash
./mvnw -pl plugin/trino-starrocks test -DskipITs -DfailIfNoTests=false
```

## Query performance optimizations implemented

### O1. Limit and TopN pushdown

- Added connector-level `applyLimit` and `applyTopN` pushdown handling.
- `StarrocksTableHandle` now carries pushed-down `limit` and `sortOrder` state.
- FE SQL generation now emits `ORDER BY` and `LIMIT` when present.

Impact:

- less data transferred from StarRocks to Trino for top-k queries,
- lower Trino worker CPU for order+limit workloads,
- better response times for dashboard-style queries.

### O2. Split compaction (tablet grouping)

- Added `scan-tablets-per-split` configuration.
- Split builder now groups multiple tablets per split instead of one-tablet-per-split.

Impact:

- fewer splits created and scheduled,
- reduced planning and coordinator overhead,
- improved throughput on high-tablet tables.

### O3. FE endpoint health-aware retries

- FE query-plan path now chooses endpoints with temporary failure penalties.
- Added jittered exponential backoff between retries.
- Endpoint success/failure feedback now influences subsequent selection.

Impact:

- lower tail latency under transient FE node issues,
- fewer avoidable query failures due to endpoint flaps,
- better resilience in multi-FE deployments.

## Pushdown scope

- Pushdown applies to query fragments handled by this StarRocks connector.
- Cross-catalog joins (for example StarRocks joined with PostgreSQL) are executed by Trino and are not fully join-pushed into StarRocks.
- Best performance is achieved when heavy filtering, topN, aggregation, and joins stay within StarRocks-side query fragments.

## Developer runbook: running the server locally

The stable server lives at `~/trino-server-480-SNAPSHOT/`. Config (including `starrocks.properties`) lives there permanently and is never overwritten by rebuilds. Connect to it from DBeaver using the Trino driver at `localhost:8080`.

### If only `starrocks.properties` changed (no code changes)

```bash
# Edit the config directly in the stable server directory
nano ~/trino-server-480-SNAPSHOT/etc/catalog/starrocks.properties

# Restart
cd ~/trino-server-480-SNAPSHOT
bin/launcher stop && bin/launcher start
```

### If connector code changed

```bash
cd /Users/stevenchung/Documents/Work/energywell/trino

# 1. Build and test the plugin — use install, not package
./mvnw -pl plugin/trino-starrocks test -DskipITs -DfailIfNoTests=false
./mvnw -pl plugin/trino-starrocks -DskipTests install

# 2. Rebuild the server distribution
./mvnw -pl core/trino-server -am -DskipTests clean package

# 3. Copy the new plugins into the stable server (etc/ is not touched)
cp -r core/trino-server/target/trino-server-480-SNAPSHOT/plugin ~/trino-server-480-SNAPSHOT/

# 4. Restart
cd ~/trino-server-480-SNAPSHOT
bin/launcher stop && bin/launcher start
```

### Verify startup

```bash
curl -s http://localhost:8080/v1/info
```

### Notes

- Use `install` in step 1, not `package`. `package` only builds the jar locally; `install` puts it in the local Maven repository so the server rebuild in step 2 picks it up instead of pulling the older remote snapshot.
- Only `plugin/` is copied in step 3 — `etc/` is left alone so your config changes are preserved.
- The stable server directory (`~/trino-server-480-SNAPSHOT/`) is set up once. If you need to recreate it from scratch: `tar -xzf core/trino-server/target/trino-server-480-SNAPSHOT.tar.gz -C ~/` then copy `etc/` in manually.

## Resolved since last assessment

### R1. DI wiring corrected

- `StarrocksConnctor` now uses injected split/page providers and table properties.
- Status: resolved.
- Impact: cleaner lifecycle behavior and better maintainability.

### R2. Config model normalized and expanded

- `StarrocksConfig` now exposes and validates timeout/scanner/retry settings.
- Status: resolved.
- Impact: operators can tune network and scanner behavior safely.

### R3. Metadata null handling fixed

- `Optional.ofNullable` now used for nullable metadata comment/extra fields.
- Status: resolved.
- Impact: avoids runtime NPE on legitimate metadata values.

### R4. Table handle equality contract fixed

- `hashCode` now aligns with `equals` field set.
- Status: resolved.
- Impact: removes planner/cache nondeterminism risk.

### R5. Timeout and error typing hardening

- FE and stream-load HTTP clients now use explicit connect/read/write/call timeouts.
- BE scanner now uses configured timeout and typed connector exceptions.
- Page source and split source now emit typed `TrinoException` in failure paths.
- Status: largely resolved.
- Impact: reduced hang risk and improved observability.

### R6. Test drift fixed and regressions added

- Endpoint expectation test aligned with implementation.
- Added regression tests for metadata null handling and table-handle equality contract.
- Status: resolved.

## Resolved blockers

### P0-A. Transactional write semantics with explicit commit/abort

Status: resolved.

What changed:

- `StarrocksPageSink` now uses StarRocks Stream Load transaction endpoints:
- `/api/transaction/begin`
- `/api/transaction/load` (chunked writes)
- `/api/transaction/prepare`
- `/api/transaction/commit`
- `/api/transaction/rollback`
- `finish()` now performs `prepare + commit`.
- `abort()` now performs explicit `rollback`.
- buffering is bounded by configured `scan-batch-rows`, avoiding unbounded memory growth.

Impact:

- explicit atomic lifecycle for connector-managed writes,
- lower OOM risk on large inserts,
- clearer behavior on failure paths.

### P1-A. Failure-path coverage for transaction lifecycle

Status: resolved for connector-level transaction logic.

What changed:

- Added `StarrocksPageSinkTransactionTest` with failure-oriented scenarios:
- successful `begin/load/prepare/commit` flow,
- explicit rollback on `abort()`,
- rollback attempted when commit fails.

Impact:

- key transaction control flows are now exercised automatically,
- regressions in commit/rollback sequencing are caught by tests.

Scope note:

- these are fast connector-level tests using an in-process HTTP stub.
- full end-to-end cluster fault-injection remains recommended for very high-risk production environments.

## High-priority hardening still recommended

### H1. Security transport posture

- Endpoint normalization still defaults missing scheme to `http://`.
- Recommendation: require explicit scheme or prefer `https://` default for production.

### H2. SQL log redaction

- FE client logs full DDL/UPDATE SQL on failures.
- Recommendation: redact literals or log structured metadata instead.

### H3. Endpoint health awareness

- FE query-plan path now uses health-aware endpoint selection with temporary failure penalties.
- Recommendation: extend the same policy to all FE/StreamLoad call paths for consistency.

## Updated severity view

- **P2**
- Secure transport defaulting and strict TLS posture.
- SQL log redaction.
- Endpoint health-aware selection.
- Naming cleanup (`StarrocksConnctor` typo).

## Exit criteria for production readiness

Connector is considered production-ready for staged rollout now that P0/P1 blockers are closed.

For broader enterprise hardening, complete the following:

1. Enforce stricter transport posture (prefer explicit `https://` endpoint configuration).
2. Add SQL log redaction for potentially sensitive literals.
3. Add health-aware FE endpoint rotation.
4. Add full end-to-end fault-injection tests in a real StarRocks test environment.

## Bottom line

The connector is now at a safe baseline for staged production use, with transactional writes and explicit rollback/commit behavior implemented and tested. Remaining items are hardening improvements rather than release blockers.
