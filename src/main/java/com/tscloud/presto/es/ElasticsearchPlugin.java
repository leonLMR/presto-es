package com.tscloud.presto.es;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;

import static com.google.common.base.MoreObjects.firstNonNull;

/**
 * Created by Administrator on 2016/11/15.
 */
public class ElasticsearchPlugin implements Plugin {

    public static final String name="es";

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return ImmutableList.of(new ElasticsearchConnectorFactory( getClassLoader() ) );
    }

    private static ClassLoader getClassLoader() {
        return firstNonNull(Thread.currentThread().getContextClassLoader(), ElasticsearchPlugin.class.getClassLoader());
    }

}
