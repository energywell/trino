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

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;

public class StarrocksConnctor
        implements Connector
{
    private final LifeCycleManager lifeCycleManager;
    private final StarrocksClient client;
    private final StarrocksConfig config;
    private final StarrocksTypeMapper typeMapper;
    private final StarrocksSplitManager splitManager;
    private final StarrocksPageSourceProvider pageSourceProvider;
    private final StarrocksPageSinkProvider pageSinkProvider;
    private final List<PropertyMetadata<?>> sessionProperties;
    private final StarrocksTableProperties tableProperties;
    private final Map<StarrocksTransactionHandle, StarrocksMetadata> transactionMetadata = new ConcurrentHashMap<>();

    @Inject
    public StarrocksConnctor(
            LifeCycleManager lifeCycleManager,
            StarrocksClient client,
            StarrocksConfig config,
            StarrocksSessionProperties sessionProperties,
            StarrocksTypeMapper typeMapper)
    {
        this.lifeCycleManager = lifeCycleManager;
        this.client = client;
        this.config = config;
        this.typeMapper = typeMapper;
        this.splitManager = new StarrocksSplitManager(client);
        this.pageSourceProvider = new StarrocksPageSourceProvider(client, typeMapper);
        this.pageSinkProvider = new StarrocksPageSinkProvider(config);
        this.sessionProperties = sessionProperties.getSessionProperties();
        this.tableProperties = new StarrocksTableProperties();
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        StarrocksTransactionHandle handle = new StarrocksTransactionHandle();
        transactionMetadata.put(handle, new StarrocksMetadata(client, config, typeMapper));
        return handle;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        StarrocksMetadata metadata = transactionMetadata.get((StarrocksTransactionHandle) transactionHandle);
        if (metadata == null) {
            throw new IllegalStateException("No metadata for transaction handle: " + transactionHandle);
        }
        return metadata;
    }

    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        StarrocksMetadata metadata = transactionMetadata.remove((StarrocksTransactionHandle) transactionHandle);
        if (metadata != null) {
            metadata.clearRollback();
        }
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        StarrocksMetadata metadata = transactionMetadata.remove((StarrocksTransactionHandle) transactionHandle);
        if (metadata != null) {
            metadata.rollback();
        }
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public void shutdown()
    {
        lifeCycleManager.stop();
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties.getTableProperties();
    }
}
