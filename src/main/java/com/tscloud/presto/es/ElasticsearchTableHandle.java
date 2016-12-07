package com.tscloud.presto.es;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;

import java.util.Objects;

/**
 * Created by Administrator on 2016/11/16.
 */
public class ElasticsearchTableHandle implements ConnectorTableHandle {
    private String schemaName;
    private String tableName;

    @JsonCreator
    public ElasticsearchTableHandle( @JsonProperty("schemaName") String schemaName,
                                     @JsonProperty("tableName") String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( schemaName, tableName );
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        ElasticsearchTableHandle other = (ElasticsearchTableHandle) obj;
        return  Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName);
    }

    @Override
    public String toString()
    {
        return Joiner.on(":").join( schemaName, tableName);
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }
}
