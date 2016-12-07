package com.tscloud.presto.es;

import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.tscloud.presto.es.exception.ElasticsearchException;
import io.airlift.slice.Slice;
import org.elasticsearch.action.bulk.BulkRequestBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Created by Administrator on 2016/11/21.
 */
public class ElasticsearchPageSink implements ConnectorPageSink {

    private final String schema;
    private final String table;

    private final List<ElasticsearchColumn> cloumns;
    private final ElasticsearchClient client;
    private final BulkRequestBuilder bulk;
    public ElasticsearchPageSink(String schema, String table, List<ElasticsearchColumn> cloumns, ElasticsearchClient client) {
        this.schema = schema;
        this.table = table;
        this.cloumns = cloumns;
        this.client = client;
        this.bulk = client.getBulkRequest();
    }

    @Override
    public CompletableFuture<?> appendPage( Page page, Block block ) {
        for ( int i = 0; i < page.getPositionCount(); i++ ){
            Map<String,Object> map = new HashMap<String,Object>();
            for ( int j = 0; j < page.getChannelCount(); j++ ) {
                ElasticsearchColumn columnHandle = cloumns.get(j);
                Type type = columnHandle.getType();
                map.put( columnHandle.getName(), getObjectValue(type, page.getBlock( j ), i ) );
            }
            try {
                bulk.add( client.getIndexBuilder( schema, table, map ) );
            } catch (IOException e) {
                throw new ElasticsearchException( e );
            }
        }
        return NOT_BLOCKED;
    }

    private Object getObjectValue( Type type, Block block, int position ) {
        if (block.isNull(position)) {
            return null;
        }
        Object value = readNativeValue( type, block, position );
        return value;
    }

    private Object readNativeValue(Type type, Block block, int position) {
        Class<?> javaType = type.getJavaType();

        if (block.isNull(position)) {
            return null;
        }
        if (javaType == long.class) {
            return type.getLong(block, position);
        }
        if (javaType == double.class) {
            return type.getDouble(block, position);
        }
        if (javaType == boolean.class) {
            return type.getBoolean(block, position);
        }
        if (javaType == Slice.class) {
            Slice slice = type.getSlice(block, position);
            return new String( slice.getBytes() );
        }
        return type.getObject(block, position);
    }

    @Override
    public Collection<Slice> finish() {
        bulk.execute().actionGet();
        return ImmutableList.of();
    }



    @Override
    public void abort() {

    }
}
