package com.tscloud.presto.es;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.tscloud.presto.es.utils.ClassCastUtil;

/**
 * Created by Administrator on 2016/11/17.
 */
public class ElasticsearchSplitManager implements ConnectorSplitManager {

    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout) {
        ElasticsearchTableLayoutHandle layoutHandle = ClassCastUtil.checkType(layout, ElasticsearchTableLayoutHandle.class, "split");
        ElasticsearchTableHandle table = layoutHandle.getTable();
        ElasticsearchSplit split = new ElasticsearchSplit( table.getSchemaName(), table.getTableName(), layoutHandle.getTupleDomain() );
        return new FixedSplitSource( ImmutableList.of( split ) );
    }

}
