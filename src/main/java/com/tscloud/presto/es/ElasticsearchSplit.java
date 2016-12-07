package com.tscloud.presto.es;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Created by Administrator on 2016/11/17.
 */
public class ElasticsearchSplit implements ConnectorSplit {

    private final String schemaName;
    private final String tableName;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final boolean remotelyAccessible;

    @JsonCreator
    public ElasticsearchSplit( @JsonProperty("schemaName") String schemaName,
                               @JsonProperty("tableName") String tableName,
                               @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain ) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.remotelyAccessible = true;
        this.tupleDomain = tupleDomain;
    }

    @Override
    public boolean isRemotelyAccessible() {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo() {
        return this;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain() {
        return tupleDomain;
    }
}
