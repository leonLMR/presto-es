package com.tscloud.presto.es;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class ElasticsearchColumnHandle
        implements ColumnHandle
{
    private final String name;
    private final Type type;
    private final String esName;
    private final String esType;

    private final int ordinalPosition;

    @JsonCreator
    public ElasticsearchColumnHandle(
            @JsonProperty("name") String columnName,
            @JsonProperty("type") Type columnType,
            @JsonProperty("esName") String columnJsonPath,
            @JsonProperty("esType") String columnJsonType,
            @JsonProperty("ordinalPosition") int ordinalPosition)
    {
        this.name = requireNonNull(columnName, "columnName is null");
        this.type = requireNonNull(columnType, "columnType is null");
        this.esName = requireNonNull(columnJsonPath, "columnJsonPath is null");
        this.esType = requireNonNull(columnJsonType, "columnJsonType is null");
        this.ordinalPosition = ordinalPosition;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public Type getType() {
        return type;
    }

    @JsonProperty
    public String getEsName() {
        return esName;
    }

    @JsonProperty
    public String getEsType() {
        return esType;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(name, type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( name );
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

        ElasticsearchColumnHandle other = (ElasticsearchColumnHandle) obj;
        return Objects.equals( this.name, other.name );
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .add("esName", esName)
                .add("esType", esType)
                .add("ordinalPosition", ordinalPosition)
                .toString();
    }
}
