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
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.Min;

import java.util.Optional;

public class StarRocksConfig
{
    private String jdbcUrl;
    private String username;
    private String password;
    private String flightSqlHost;
    private int flightSqlPort = 9408;

    public Optional<String> getJdbcUrl()
    {
        return Optional.ofNullable(jdbcUrl);
    }

    @Config("starrocks.jdbc-url")
    @ConfigDescription("StarRocks JDBC URL used for metadata discovery")
    public StarRocksConfig setJdbcUrl(String jdbcUrl)
    {
        this.jdbcUrl = jdbcUrl;
        return this;
    }

    public Optional<String> getUsername()
    {
        return Optional.ofNullable(username);
    }

    @Config("starrocks.username")
    @ConfigDescription("StarRocks username")
    public StarRocksConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    public Optional<String> getPassword()
    {
        return Optional.ofNullable(password);
    }

    @Config("starrocks.password")
    @ConfigSecuritySensitive
    public StarRocksConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public Optional<String> getFlightSqlHost()
    {
        return Optional.ofNullable(flightSqlHost);
    }

    @Config("starrocks.flight-sql-host")
    @ConfigDescription("StarRocks FE host used for Arrow Flight SQL reads")
    public StarRocksConfig setFlightSqlHost(String flightSqlHost)
    {
        this.flightSqlHost = flightSqlHost;
        return this;
    }

    @Min(1)
    public int getFlightSqlPort()
    {
        return flightSqlPort;
    }

    @Config("starrocks.flight-sql-port")
    @ConfigDescription("StarRocks FE Arrow Flight SQL port used for reads")
    public StarRocksConfig setFlightSqlPort(int flightSqlPort)
    {
        this.flightSqlPort = flightSqlPort;
        return this;
    }
}
