package com.tscloud.presto.es;

import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public final class ElasticsearchColumn
{
    private final String name;
    private final Type type;
    private final String esName;
    private final String esType;

    @JsonCreator
    public ElasticsearchColumn(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("esName") String esName,
            @JsonProperty("esType") String esType)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = name;
        this.type = requireNonNull(type, "type is null");
        this.esName = requireNonNull(esName, "esName is null");
        this.esType = requireNonNull(esType, "esType is null");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getType()
    {
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

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, esName, esType);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ElasticsearchColumn other = (ElasticsearchColumn) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.esName, other.esName) &&
                Objects.equals(this.esType, other.esType);
    }

    @Override
    public String toString()
    {
        return name + ":" + type + ":" + esName + ":" + esType;
    }
}
