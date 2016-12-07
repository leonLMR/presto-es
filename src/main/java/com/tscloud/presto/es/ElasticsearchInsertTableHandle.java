package com.tscloud.presto.es;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Created by Administrator on 2016/11/18.
 */
public class ElasticsearchInsertTableHandle extends ElasticsearchTable implements ConnectorOutputTableHandle, ConnectorInsertTableHandle {

    @JsonCreator
    public ElasticsearchInsertTableHandle( @JsonProperty("schema") String schema,
                                           @JsonProperty("name") String name,
                                           @JsonProperty("columns") List<ElasticsearchColumn> columns) {
        super(schema, name, columns);
    }

}
