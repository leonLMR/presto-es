package com.tscloud.presto.es;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tscloud.presto.es.ElasticsearchTableHandle;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class ElasticsearchTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final ElasticsearchTableHandle table;
    private final TupleDomain<ColumnHandle> tupleDomain;

    @JsonCreator
    public ElasticsearchTableLayoutHandle(
            @JsonProperty("table") ElasticsearchTableHandle table,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> domain)
    {
        this.table = requireNonNull(table, "table is null");
        this.tupleDomain = requireNonNull(domain, "tupleDomain is null");
    }

    @JsonProperty
    public ElasticsearchTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ElasticsearchTableLayoutHandle that = (ElasticsearchTableLayoutHandle) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(tupleDomain, that.tupleDomain);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, tupleDomain);
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
