package com.tscloud.presto.es;

import com.facebook.presto.spi.connector.*;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.Collections;
import java.util.List;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_COMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

/**
 * Created by Administrator on 2016/11/15.
 */
public class ElasticsearchConnector implements Connector {

    private static final Logger log = Logger.get(ElasticsearchConnector.class);
    private final LifeCycleManager lifeCycleManager;
    private final ElasticsearchSplitManager splitManager;
    private final ElasticsearchMetadata metadata;
    private final ElasticsearchRecordSetProvider recordSetProvider;
    private final ElasticsearchPageSinkProvider pageSinkProvider;
    @Inject
    public ElasticsearchConnector(LifeCycleManager lifeCycleManager,
                                  ElasticsearchSplitManager splitManager,
                                  ElasticsearchMetadata metadata,
                                  ElasticsearchRecordSetProvider recordSetProvider,
                                  ElasticsearchPageSinkProvider pageSinkProvider) {
        this.lifeCycleManager = lifeCycleManager;
        this.splitManager = splitManager;
        this.metadata = metadata;
        this.recordSetProvider = recordSetProvider;
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean b) {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return ElasticsearchTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata( ConnectorTransactionHandle connectorTransactionHandle ) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return this.recordSetProvider;
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        } catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }

    /**
     * 获取elasticsearch创建index时支持的属性
     * 如：分片数(number_of_shards)、备份数(number_of_replicas)
     * @return
     */
    @Override
    public List<PropertyMetadata<?>> getSchemaProperties() {
        return ElasticsearchSchemaProperties.SCHEMA_PROPERTIES;
    }

    /**
     * 获取elasticsearch创建doctype时支持的属性
     * 如：
     * store : true / false
     * index : analyzed / not_analyzed
     * @return
     */
    @Override
    public List<PropertyMetadata<?>> getTableProperties() {
        return ElasticsearchSchemaProperties.TABLE_PROPERTIES;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider() {
        return this.pageSinkProvider;
    }

}
