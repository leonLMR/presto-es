package com.tscloud.presto.es;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.tscloud.presto.es.utils.ClassCastUtil;

import javax.inject.Inject;

/**
 * Created by Administrator on 2016/11/21.
 */
public class ElasticsearchPageSinkProvider implements ConnectorPageSinkProvider {

    private final ElasticsearchClient client;

    @Inject
    public ElasticsearchPageSinkProvider( ElasticsearchClient client ){
        this.client = client;
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle connectorTransactionHandle, ConnectorSession connectorSession, ConnectorOutputTableHandle connectorOutputTableHandle) {
        ElasticsearchInsertTableHandle outputTableHandle = ClassCastUtil.checkType( connectorOutputTableHandle, ElasticsearchInsertTableHandle.class, "connectorOutputTableHandle" );
        return new ElasticsearchPageSink( outputTableHandle.getSchema(), outputTableHandle.getName(), outputTableHandle.getColumns(), client );
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle connectorTransactionHandle, ConnectorSession connectorSession, ConnectorInsertTableHandle connectorInsertTableHandle) {
        ElasticsearchInsertTableHandle outputTableHandle = ClassCastUtil.checkType( connectorInsertTableHandle, ElasticsearchInsertTableHandle.class, "connectorInsertTableHandle" );
        return new ElasticsearchPageSink( outputTableHandle.getSchema(), outputTableHandle.getName(), outputTableHandle.getColumns(), client );
    }
}
