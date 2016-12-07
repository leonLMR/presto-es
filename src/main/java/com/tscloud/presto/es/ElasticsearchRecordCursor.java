package com.tscloud.presto.es;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2016/11/17.
 */
public class ElasticsearchRecordCursor implements RecordCursor {

    private static final Logger log = Logger.get(ElasticsearchRecordCursor.class);
    private final String index;
    private final String type;
    private final ElasticsearchClient elasticsearchClient;
    private final List<ElasticsearchColumnHandle> columnHandles;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private SearchResponse sr;
    private Iterator<SearchHit> iterator;
    private List<Object> fields;

    public ElasticsearchRecordCursor( ElasticsearchClient elasticsearchClient,
                                      String index,
                                      String type,
                                      List<ElasticsearchColumnHandle> columnHandles,
                                      TupleDomain<ColumnHandle> tupleDomain ) {
        for ( ElasticsearchColumnHandle columnHandle : columnHandles ){

        }
        this.elasticsearchClient = elasticsearchClient;
        this.index = index;
        this.type = type;
        this.columnHandles = columnHandles;
        this.tupleDomain = tupleDomain;
        this.sr = elasticsearchClient.query( index, type, columnHandles, tupleDomain );
        this.iterator = sr.getHits().iterator();
    }

    @Override
    public long getTotalBytes() {
        return 0;
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int i) {
        return columnHandles.get( i ).getType();
    }

    @Override
    public boolean advanceNextPosition() {
        if ( iterator.hasNext() ){
            SearchHit sh = iterator.next();
            Map<String,Object> source = sh.getSource();
            fields = new ArrayList();
            for ( ElasticsearchColumnHandle ch: columnHandles ) {
                fields.add( source.get( ch.getName() ) );
            }
            return true;
        } else {
            this.sr = elasticsearchClient.scroll( sr.getScrollId() );
            if ( sr.getHits().getHits().length == 0 ){
                return false;
            }
            this.iterator = sr.getHits().iterator();
            return advanceNextPosition();
        }
    }

    @Override
    public boolean getBoolean(int i) {
        return (Boolean) fields.get( i );
    }

    @Override
    public long getLong(int i) {
        return Long.valueOf( String.valueOf( fields.get( i ) ) );
    }

    @Override
    public double getDouble(int i) {
        return (Double) fields.get( i );
    }

    @Override
    public Slice getSlice(int i) {
        return Slices.utf8Slice( String.valueOf( fields.get(i) ) );
    }

    @Override
    public Object getObject(int i) {
        return fields.get( i );
    }

    @Override
    public boolean isNull(int i) {
        return fields.get( i ) == null;
    }

    @Override
    public void close() {

    }
}
