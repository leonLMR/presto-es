package com.tscloud.presto.es;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.log.Logger;

import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * Created by Administrator on 2016/11/15.
 */
public class ElasticsearchModule implements Module {

    private static final Logger log = Logger.get(ElasticsearchModule.class);

    private final String connectorId;

    public ElasticsearchModule( String connectorId ){
        this.connectorId = connectorId;
    }

    @Override
    public void configure( Binder binder ) {
        binder.bind( ElasticsearchConnector.class ).in( Scopes.SINGLETON );
        binder.bind( ElasticsearchMetadata.class ).in( Scopes.SINGLETON );
        binder.bind( ElasticsearchClient.class ).in( Scopes.SINGLETON );
        binder.bind( ElasticsearchSplitManager.class ).in( Scopes.SINGLETON );
        binder.bind( ElasticsearchRecordSetProvider.class ).in( Scopes.SINGLETON );
        binder.bind( ElasticsearchPageSinkProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig( ElasticsearchConfig.class );
    }

}
