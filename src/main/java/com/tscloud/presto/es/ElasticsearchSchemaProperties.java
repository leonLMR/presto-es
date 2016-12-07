package com.tscloud.presto.es;

import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;

/**
 * Created by Administrator on 2016/11/18.
 */
public final class ElasticsearchSchemaProperties {

    private ElasticsearchSchemaProperties() {}

    /**
     * schema properties
     */
    public static final String NUMBER_OF_SHARDS = "number_of_shards";
    public static final String NUMBER_OF_REPLICAS = "number_of_replicas";

    public static final List<PropertyMetadata<?>> SCHEMA_PROPERTIES = ImmutableList.of(
            integerSessionProperty(
                    NUMBER_OF_SHARDS,
                    "number of shards",
                    null,
                    false),
            integerSessionProperty(
                    NUMBER_OF_REPLICAS,
                    "number of replicas",
                    null,
                    false));

    /**
     * table properties
     */
    public static final String STORE = "store";
    public static final String INDEX = "index";
    public static final String FORMAT = "format";
    public static final String NULL_VALUE = "null_value";

    public static final List<PropertyMetadata<?>> TABLE_PROPERTIES = ImmutableList.of(
            stringSessionProperty(
                    STORE,
                    "store : true / false",
                    null,
                    false),
            stringSessionProperty(
                    INDEX,
                    "index : analyzed / not_analyzed",
                    null,
                    false),
            stringSessionProperty(
                    FORMAT,
                    "format : YYYY-MM-dd",
                    null,
                    false),
            stringSessionProperty(
                    NULL_VALUE,
                    "null_value",
                    null,
                    false));

}
