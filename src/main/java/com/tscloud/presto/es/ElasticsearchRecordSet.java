package com.tscloud.presto.es;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Created by Administrator on 2016/11/17.
 */
public class ElasticsearchRecordSet implements RecordSet {

    private final List<ElasticsearchColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final ElasticsearchClient elasticsearchClient;
    private final ElasticsearchSplit split;

    public ElasticsearchRecordSet(ElasticsearchSplit split, List<ElasticsearchColumnHandle> columnHandles, ElasticsearchClient elasticsearchClient)
    {
        requireNonNull(split, "split is null");
        this.elasticsearchClient = requireNonNull(elasticsearchClient, "client is null");
        this.split = requireNonNull(split, "split is null");

        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for ( ElasticsearchColumnHandle column : columnHandles ) {
            types.add( column.getType() );
        }
        this.columnTypes = types.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new ElasticsearchRecordCursor( elasticsearchClient, split.getSchemaName(), split.getTableName()
                                                , columnHandles, split.getTupleDomain() );
    }
}
