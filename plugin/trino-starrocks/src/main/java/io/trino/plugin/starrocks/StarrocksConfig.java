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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

public class StarrocksConfig
{
    private String scanURL;
    private String jdbcURL;
    private String username;
    private Optional<String> password = Optional.empty();
    private Duration scanConnectTimeout = Duration.valueOf("1s");
    private Duration scanReadTimeout = Duration.valueOf("30s");
    private Duration scanWriteTimeout = Duration.valueOf("30s");
    private int scanBatchRows = 1000;
    private int scanTabletsPerSplit = 16;
    private Duration scanKeepAlive = Duration.valueOf("1m");
    private Duration scanQueryTimeout = Duration.valueOf("10m");
    private int scanMaxRetry = 3;
    private Duration dynamicFilteringWaitTimeout = Duration.valueOf("10s");
    private int tupleDomainLimit = 1000;

    @NotNull
    public String getScanURL()
    {
        return scanURL;
    }

    @Config("scan-url")
    @ConfigDescription("Scan URL for the Starrocks BE")
    public StarrocksConfig setScanURL(String scanURL)
    {
        this.scanURL = scanURL;
        return this;
    }

    @NotNull
    public String getJdbcURL()
    {
        return jdbcURL;
    }

    @Config("jdbc-url")
    @ConfigDescription("JDBC URL for the Starrocks FE")
    public StarrocksConfig setJdbcURL(String jdbcURL)
    {
        this.jdbcURL = jdbcURL;
        return this;
    }

    @NotNull
    public String getUsername()
    {
        return username;
    }

    @Config("username")
    @ConfigDescription("Username for the Starrocks user")
    public StarrocksConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getScanConnectTimeout()
    {
        return scanConnectTimeout;
    }

    @Config("scan-connect-timeout")
    @ConfigDescription("Timeout for establishing FE/BE network connections")
    public StarrocksConfig setScanConnectTimeout(Duration scanConnectTimeout)
    {
        this.scanConnectTimeout = scanConnectTimeout;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getScanReadTimeout()
    {
        return scanReadTimeout;
    }

    @Config("scan-read-timeout")
    @ConfigDescription("Timeout for FE/StreamLoad HTTP reads")
    public StarrocksConfig setScanReadTimeout(Duration scanReadTimeout)
    {
        this.scanReadTimeout = scanReadTimeout;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getScanWriteTimeout()
    {
        return scanWriteTimeout;
    }

    @Config("scan-write-timeout")
    @ConfigDescription("Timeout for FE/StreamLoad HTTP writes")
    public StarrocksConfig setScanWriteTimeout(Duration scanWriteTimeout)
    {
        this.scanWriteTimeout = scanWriteTimeout;
        return this;
    }

    @Min(1)
    public int getScanBatchRows()
    {
        return scanBatchRows;
    }

    @Config("scan-batch-rows")
    @ConfigDescription("Batch row count used by StarRocks BE scanner")
    public StarrocksConfig setScanBatchRows(int scanBatchRows)
    {
        this.scanBatchRows = scanBatchRows;
        return this;
    }

    @Min(1)
    public int getScanTabletsPerSplit()
    {
        return scanTabletsPerSplit;
    }

    @Config("scan-tablets-per-split")
    @ConfigDescription("Maximum number of StarRocks tablets grouped into one Trino split")
    public StarrocksConfig setScanTabletsPerSplit(int scanTabletsPerSplit)
    {
        this.scanTabletsPerSplit = scanTabletsPerSplit;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getScanKeepAlive()
    {
        return scanKeepAlive;
    }

    @Config("scan-keep-alive")
    @ConfigDescription("Scanner keep-alive duration")
    public StarrocksConfig setScanKeepAlive(Duration scanKeepAlive)
    {
        this.scanKeepAlive = scanKeepAlive;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getScanQueryTimeout()
    {
        return scanQueryTimeout;
    }

    @Config("scan-query-timeout")
    @ConfigDescription("Query timeout used by FE query plan and BE scanner")
    public StarrocksConfig setScanQueryTimeout(Duration scanQueryTimeout)
    {
        this.scanQueryTimeout = scanQueryTimeout;
        return this;
    }

    @Config("dynamic-filtering-wait-timeout")
    @ConfigDescription("Duration to wait for completion of dynamic filters")
    public StarrocksConfig setDynamicFilteringWaitTimeout(Duration dynamicFilteringWaitTimeout)
    {
        this.dynamicFilteringWaitTimeout = dynamicFilteringWaitTimeout;
        return this;
    }

    @Min(1)
    public int getTupleDomainLimit()
    {
        return tupleDomainLimit;
    }

    @Config("tuple-domain-limit")
    @ConfigDescription("Maximum number of tuple domains to include in a single dynamic filter")
    public StarrocksConfig setTupleDomainLimit(int tupleDomainLimit)
    {
        this.tupleDomainLimit = tupleDomainLimit;
        return this;
    }

    public Optional<String> getPassword()
    {
        return password;
    }

    @Config("password")
    @ConfigDescription("Password for the Starrocks user")
    public StarrocksConfig setPassword(String password)
    {
        this.password = Optional.ofNullable(password);
        return this;
    }

    @Min(1)
    public int getScanMaxRetries()
    {
        return scanMaxRetry;
    }

    @Config("scan-max-retries")
    @ConfigDescription("Maximum retry count for FE query plan requests")
    public StarrocksConfig setScanMaxRetries(int scanMaxRetry)
    {
        this.scanMaxRetry = scanMaxRetry;
        return this;
    }

    @MinDuration("0ms")
    @NotNull
    public Duration getDynamicFilteringWaitTimeout()
    {
        return dynamicFilteringWaitTimeout;
    }
}
