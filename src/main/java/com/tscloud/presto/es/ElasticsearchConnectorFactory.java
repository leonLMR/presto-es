package com.tscloud.presto.es;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Created by Administrator on 2016/11/15.
 */
public class ElasticsearchConnectorFactory implements ConnectorFactory {

    private final ClassLoader classLoader;

    public ElasticsearchConnectorFactory( ClassLoader classLoader ) {
        this.classLoader = requireNonNull( classLoader, "classLoader is null" );
    }

    @Override
    public String getName() {
        return ElasticsearchPlugin.name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return new ElasticsearchHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> map, ConnectorContext connectorContext) {
        try ( ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader) ) {
            Bootstrap app = new Bootstrap(
                new JsonModule(),
                new ElasticsearchModule(connectorId),
                binder -> {
                    binder.bind(TypeManager.class).toInstance(connectorContext.getTypeManager());
                    binder.bind(NodeManager.class).toInstance(connectorContext.getNodeManager());
                });

            Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties( map )
                .initialize();

            return injector.getInstance(ElasticsearchConnector.class);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
