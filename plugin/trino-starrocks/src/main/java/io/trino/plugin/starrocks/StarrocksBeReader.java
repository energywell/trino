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

import com.starrocks.shade.org.apache.thrift.TException;
import com.starrocks.shade.org.apache.thrift.protocol.TBinaryProtocol;
import com.starrocks.shade.org.apache.thrift.protocol.TProtocol;
import com.starrocks.shade.org.apache.thrift.transport.TSocket;
import com.starrocks.shade.org.apache.thrift.transport.TTransportException;
import com.starrocks.thrift.TScanBatchResult;
import com.starrocks.thrift.TScanCloseParams;
import com.starrocks.thrift.TScanNextBatchParams;
import com.starrocks.thrift.TScanOpenParams;
import com.starrocks.thrift.TScanOpenResult;
import com.starrocks.thrift.TStarrocksExternalService;
import com.starrocks.thrift.TStatusCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class StarrocksBeReader
        implements AutoCloseable
{
    private final String ip;
    private final int port;
    private final StarrocksConfig config;
    private final SchemaTableName schemaTableName;
    private final List<ColumnHandle> columnHandle;
    private final TSocket socket;
    private String contextId;
    private int readerOffset;
    private TStarrocksExternalService.Client client;

    StarrocksBeReader(
            StarrocksConfig config,
            String beInfo,
            List<ColumnHandle> columns,
            SchemaTableName schemaTableName)
    {
        this.config = requireNonNull(config, "config is null");
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.columnHandle = requireNonNull(columns, "columns is null");

        String[] beNode = beInfo.split(":");
        String ip = beNode[0].trim();
        int port = Integer.parseInt(beNode[1].trim());

        TBinaryProtocol.Factory factory = new TBinaryProtocol.Factory();
        int socketTimeoutMillis = (int) Math.max(1, Math.min(Integer.MAX_VALUE, this.config.getScanConnectTimeout().toMillis()));
        this.socket = new TSocket(ip, port, socketTimeoutMillis);
        try {
            this.socket.open();
        }
        catch (TTransportException e) {
            this.socket.close();
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to open scanner socket to " + ip + ":" + port, e);
        }
        TProtocol protocol = factory.getProtocol(this.socket);
        this.ip = ip;
        this.port = port;
        this.client = new TStarrocksExternalService.Client(protocol);
    }

    public TStarrocksExternalService.Client getClient()
    {
        return client;
    }

    public void openScanner(
            List<Long> tablets,
            String opaquedQueryPlan)
    {
        TScanOpenParams params = new TScanOpenParams();
        params.setTablet_ids(tablets);
        params.setCluster("default_cluster");
        params.setOpaqued_query_plan(opaquedQueryPlan);
        params.setDatabase(schemaTableName.getSchemaName());
        params.setTable(schemaTableName.getTableName());
        params.setUser(config.getUsername());
        params.setPasswd(config.getPassword().orElse(null));
        params.setBatch_size(config.getScanBatchRows());
        short keepAliveMin = (short) Math.min(Short.MAX_VALUE, config.getScanKeepAlive().roundTo(MINUTES));
        params.setKeep_alive_min(keepAliveMin);
        params.setQuery_timeout((int) Math.min(Integer.MAX_VALUE, config.getScanQueryTimeout().roundTo(SECONDS)));
        params.setMem_limit(1024 * 1024 * 1024L);
        TScanOpenResult result = null;
        try {
            result = client.open_scanner(params);
            if (!result.getStatus().getStatus_code().equals(TStatusCode.OK)) {
                throw new TrinoException(
                        GENERIC_INTERNAL_ERROR,
                        "Failed to open scanner."
                                + result.getStatus().getStatus_code()
                                + result.getStatus().getError_msgs());
            }
        }
        catch (TException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to open scanner.", e);
        }
        this.contextId = result.getContext_id();
    }

    public TScanBatchResult getNextBatch()
    {
        TScanNextBatchParams params = new TScanNextBatchParams();
        params.setContext_id(this.contextId);
        params.setOffset(this.readerOffset);
        TScanBatchResult result;
        try {
            result = client.get_next(params);
            if (!TStatusCode.OK.equals(result.getStatus().getStatus_code())) {
                throw new TrinoException(
                        GENERIC_INTERNAL_ERROR,
                        "Failed to get next from be -> ip:[" + ip + "] "
                                + result.getStatus().getStatus_code() + " msg:" + result.getStatus().getError_msgs());
            }
        }
        catch (TException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to fetch scanner batch from " + ip + ":" + port, e);
        }
        return result;
    }

    public int getReaderOffset()
    {
        return this.readerOffset;
    }

    public void setReaderOffset(int readerOffset)
    {
        this.readerOffset = readerOffset;
    }

    @Override
    public void close()
    {
        if (this.contextId != null) {
            TScanCloseParams tScanCloseParams = new TScanCloseParams();
            tScanCloseParams.setContext_id(this.contextId);
            try {
                this.client.close_scanner(tScanCloseParams);
            }
            catch (TException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to close scanner context " + contextId, e);
            }
            finally {
                this.contextId = null;
            }
        }
        this.socket.close();
    }
}
