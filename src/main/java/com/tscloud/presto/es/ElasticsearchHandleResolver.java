package com.tscloud.presto.es;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

/**
 * Created by Administrator on 2016/11/17.
 */
public class ElasticsearchHandleResolver implements ConnectorHandleResolver {

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass() {
        return ElasticsearchTableHandle.class;
    }

    @Override
    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass() {
        return ElasticsearchTableLayoutHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass() {
        return ElasticsearchColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass() {

        return ElasticsearchSplit.class;
    }

    @Override
    public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass() {
        return ElasticsearchTransactionHandle.class;
    }

    @Override
    public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass() {
        return ElasticsearchInsertTableHandle.class;
    }
}
