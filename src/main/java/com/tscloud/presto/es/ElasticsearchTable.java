package com.tscloud.presto.es;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Created by Administrator on 2016/11/16.
 */
public class ElasticsearchTable {

    private String schema;

    private String name;

    private List<ElasticsearchColumn> columns;

    @JsonCreator
    public ElasticsearchTable( @JsonProperty("schema") String schema,
                               @JsonProperty("name") String name,
                               @JsonProperty("columns") List<ElasticsearchColumn> columns) {
        this.schema = schema;
        this.name = name;
        this.columns = columns;
    }

    @JsonProperty
    public String getSchema() {
        return schema;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public List<ElasticsearchColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<ElasticsearchColumn> columns) {
        this.columns = columns;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public void setName(String name) {
        this.name = name;
    }

}
