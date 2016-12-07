package com.tscloud.presto.es;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.tscloud.presto.es.utils.ClassCastUtil;

import javax.inject.Inject;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Created by Administrator on 2016/11/17.
 */
public class ElasticsearchRecordSetProvider implements ConnectorRecordSetProvider {

    private final ElasticsearchClient elasticsearchClient;

    @Inject
    public ElasticsearchRecordSetProvider( ElasticsearchClient elasticsearchClient ) {
        this.elasticsearchClient = requireNonNull(elasticsearchClient, "client is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle connectorTransactionHandle, ConnectorSession connectorSession, ConnectorSplit connectorSplit, List<? extends ColumnHandle> columns) {

        requireNonNull( connectorSplit, "partitionChunk is null" );
        ElasticsearchSplit elasticsearchSplit = ClassCastUtil.checkType( connectorSplit, ElasticsearchSplit.class, "split" );

        ImmutableList.Builder<ElasticsearchColumnHandle> handles = ImmutableList.builder();
        for ( ColumnHandle handle : columns ) {
            handles.add( ClassCastUtil.checkType(handle, ElasticsearchColumnHandle.class, "handle") );
        }

        return new ElasticsearchRecordSet(elasticsearchSplit, handles.build(), elasticsearchClient);

    }
}
