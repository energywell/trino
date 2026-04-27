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