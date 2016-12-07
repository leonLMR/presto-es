package com.tscloud.presto.es;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.tscloud.presto.es.exception.ElasticsearchConfigException;
import com.tscloud.presto.es.utils.TypeMapping;
import io.airlift.slice.Slice;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by Administrator on 2016/11/15.
 */
public class ElasticsearchClient {

    private static final int SCROLL_TIME = 60000;
    private static final int SCROLL_SIZE = 5000;

    private Client client;
    private ElasticsearchConfig config;

    @Inject
    public ElasticsearchClient( ElasticsearchConfig config ){
        this.config = config;
        this.client = createClient();
    }

    private Client createClient( ){
        if ( config.getHosts() == null ){
            throw new ElasticsearchConfigException( "must set parameter:client.hosts" );
        }
        if ( config.getClusterName() == null &&
                ( config.getIgnoreClusterName() == null || config.getIgnoreClusterName() == false ) ){
            throw new ElasticsearchConfigException( "set parameter \"cluster.name\" " +
                                                "or set \"client.transport.ignore_cluster_name=false\"" );
        }
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
                .put(ElasticsearchConfig.CLIENT_TRANSPORT_SNIFF, config.getTransportSniff())
                .put( ElasticsearchConfig.CLUSTER_NAME, config.getClusterName() );
        if ( config.getInterval() != null ){
            builder.put( ElasticsearchConfig.CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL, config.getInterval() );
        }
        if ( config.getIgnoreClusterName() != null ){
            builder.put( ElasticsearchConfig.CLIENT_TRANSPORT_IGNORE_CLUSTRER_NAME, config.getIgnoreClusterName() );
        }
        if ( config.getPingTimeOut() != null ){
            builder.put( ElasticsearchConfig.CLIENT_TRANSPORT_PING_TIMEOUT, config.getPingTimeOut() );
        }
        Settings settings = builder.build();
        TransportClient client =new TransportClient(settings) ;

        String hostStr = config.getHosts();
        String[] hosts = hostStr.split(",");
        for ( String host : hosts ){
            String[] hs = host.split(":");
            InetSocketTransportAddress address = new InetSocketTransportAddress( hs[0], Integer.parseInt( hs[1] ) );
            client.addTransportAddress( address );
        }
        return client;
    }

    public String[] listIndexs(){
        GetMappingsResponse response = client.admin().indices().prepareGetMappings().execute().actionGet();
        return response.getMappings().keys().toArray( String.class );
    }

    public ElasticsearchTable getTable( String schema, String table ) throws IOException {
        GetMappingsResponse res = client.admin().indices().prepareGetMappings( schema ).setTypes( table ).execute().actionGet();
        if ( res.getMappings().size() > 0 ){
            Map<String, Object> map = res.getMappings().get( schema ).get( table ).getSourceAsMap();
            Map<String, Object> properties = (Map<String, Object>) map.get("properties");
            Iterator<String> iterator = properties.keySet().iterator();
            List<ElasticsearchColumn> list = new ArrayList<ElasticsearchColumn>();
            while ( iterator.hasNext() ){
                String name = iterator.next();
                Map<String,Object> content = (Map<String, Object>) properties.get( name );
                String type = (String) content.get("type");
                Type prestoType = TypeMapping.getType( type );
                list.add( new ElasticsearchColumn( name, prestoType, name, type ) );
            }
            return new ElasticsearchTable( schema, table, list );
        }
        return null;
    }

    public List<SchemaTableName> getTables( String schema ){
        List<SchemaTableName> list = new ArrayList<SchemaTableName>();
        GetMappingsResponse res = null;
        if ( schema != null ){
            res = client.admin().indices().prepareGetMappings( schema ).execute().actionGet();
        } else {
            res = client.admin().indices().prepareGetMappings().execute().actionGet();
        }
        String[] schemas = res.getMappings().keys().toArray( String.class );
        for ( String s : schemas ){
            String[] tables = res.getMappings().get( s ).keys().toArray( String.class );
            for ( String table : tables ) {
                list.add( new SchemaTableName( s, table ) );
            }
        }
        return list;
    }

    public Map<SchemaTableName, List<ColumnMetadata>> getTableWithColumns( String schema, String type ) throws IOException {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableBuilder = ImmutableMap.builder();
        GetMappingsResponse res = null;
        if ( schema != null && type != null ){
            res = client.admin().indices().prepareGetMappings( schema ).setTypes( type ).execute().actionGet();
        } else if ( schema != null ){
            res = client.admin().indices().prepareGetMappings( schema ).execute().actionGet();
        } else {
            res = client.admin().indices().prepareGetMappings().execute().actionGet();
        }
        String[] tables = res.getMappings().get( schema ).keys().toArray(String.class);
        for ( String table : tables ) {
            Map<String, Object> map = res.getMappings().get( schema ).get( table ).getSourceAsMap();
            Map<String, Object> properties = (Map<String, Object>) map.get("properties");
            Iterator<String> iterator = properties.keySet().iterator();
            ImmutableList.Builder<ColumnMetadata> columnBuilder = ImmutableList.builder();
            while ( iterator.hasNext() ){
                String colName = iterator.next();
                Map<String,Object> col = (Map<String, Object>) properties.get( colName );
                Type prestoType = TypeMapping.getType( (String) col.get("type") );
                columnBuilder.add( new ColumnMetadata( colName, prestoType ) );
            }
            tableBuilder.put( new SchemaTableName(schema, table), columnBuilder.build() );
        }
        return tableBuilder.build();
    }

    public Map<SchemaTableName, List<ColumnMetadata>> getTablesWithColumns( String schema ) throws IOException {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableBuilder = ImmutableMap.builder();
        GetMappingsResponse res = null;
        if ( schema != null ){
            res = client.admin().indices().prepareGetMappings( schema ).execute().actionGet();
        } else {
            res = client.admin().indices().prepareGetMappings().execute().actionGet();
        }
        String[] tables = res.getMappings().get( schema ).keys().toArray(String.class);
        for ( String table : tables ) {
            Map<String, Object> map = res.getMappings().get( schema ).get( table ).getSourceAsMap();
            Map<String, Object> properties = (Map<String, Object>) map.get("properties");
            Iterator<String> iterator = properties.keySet().iterator();
            ImmutableList.Builder<ColumnMetadata> columnBuilder = ImmutableList.builder();
            while ( iterator.hasNext() ){
                String colName = iterator.next();
                Map<String,Object> col = (Map<String, Object>) properties.get( colName );
                String type = (String) col.get("type");
                Type prestoType = TypeMapping.getType( type );
                columnBuilder.add( new ColumnMetadata( colName, prestoType ) );
            }
            tableBuilder.put( new SchemaTableName(schema, table), columnBuilder.build() );
        }
        return tableBuilder.build();
    }

    public SearchResponse scroll( String scrollId ){
        return client.prepareSearchScroll( scrollId )
                .setScroll(new TimeValue(SCROLL_TIME))
                .execute()
                .actionGet();
    }

    public SearchResponse query( String index, String type, List<ElasticsearchColumnHandle> columns, TupleDomain<ColumnHandle> tupleDomain ){
        BoolFilterBuilder boolFilterBuilder = FilterBuilders.boolFilter();

        Set<Map.Entry<ColumnHandle,Domain>> set = tupleDomain.getDomains().get().entrySet();
        if ( set != null && set.size() > 0 ){
            Iterator<Map.Entry<ColumnHandle,Domain>> iterator = set.iterator();
            while ( iterator.hasNext() ) {
                Map.Entry<ColumnHandle,Domain> entry = iterator.next();
                ElasticsearchColumnHandle columnHandle = (ElasticsearchColumnHandle) entry.getKey();
                Domain domain = entry.getValue();
                boolFilterBuilder.must(addFilter(columnHandle.getEsName(), domain, columnHandle.getType()));
            }
        } else {
            boolFilterBuilder.must(FilterBuilders.matchAllFilter());
        }
        return client.prepareSearch( index )
                .setTypes( type )
                .setSearchType( SearchType.SCAN )
                .setScroll( new TimeValue( SCROLL_TIME ) )
                .setPostFilter( boolFilterBuilder )
                .execute().actionGet();
    }

    private BoolFilterBuilder addFilter(String columnName, Domain domain, Type type) {
        checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");

        BoolFilterBuilder boolFilterBuilder = FilterBuilders.boolFilter();

        if (domain.getValues().isNone() && domain.isNullAllowed()) {
            boolFilterBuilder.must(FilterBuilders.missingFilter(columnName));
        }
        else if (domain.getValues().isAll()) {
            boolFilterBuilder.must(FilterBuilders.existsFilter(columnName));
        }
        else {
            List<Object> singleValues = new ArrayList<>();
            for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
                checkState(!range.isAll()); // Already checked
                if (range.isSingleValue()) {
                    singleValues.add(range.getLow().getValue());
                }
                else {
                    if (!range.getLow().isLowerUnbounded()) {
                        switch (range.getLow().getBound()) {
                            case ABOVE:
                                boolFilterBuilder.must(FilterBuilders.rangeFilter(columnName).gt(getValue(type, range.getLow().getValue())));
                                break;
                            case EXACTLY:
                                boolFilterBuilder.must(FilterBuilders.rangeFilter(columnName).gte(getValue(type, range.getLow().getValue())));
                                break;
                            case BELOW:
                                throw new IllegalArgumentException("Low marker should never use BELOW bound");
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                        }
                    }
                    if (!range.getHigh().isUpperUnbounded()) {
                        switch (range.getHigh().getBound()) {
                            case ABOVE:
                                throw new IllegalArgumentException("High marker should never use ABOVE bound");
                            case EXACTLY:
                                boolFilterBuilder.must(FilterBuilders.rangeFilter(columnName).lte(getValue(type, range.getHigh().getValue())));
                                break;
                            case BELOW:
                                boolFilterBuilder.must(FilterBuilders.rangeFilter(columnName).lt(getValue(type, range.getHigh().getValue())));
                                break;
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                        }
                    }
                }
            }

            if (singleValues.size() == 1) {
                boolFilterBuilder.must(FilterBuilders.termFilter(columnName,  getValue(type, getOnlyElement(singleValues))));
            }
        }

        return boolFilterBuilder;
    }

    private Object getValue(Type type, Object value) {
        if ( type.equals( BigintType.BIGINT ) ) {
            return (long) value;
        } else if ( type.equals( IntegerType.INTEGER ) ) {
            return ((Number) value).intValue();
        } else if ( type.equals( DoubleType.DOUBLE ) ) {
            return (double) value;
        } else if ( type.equals( VarcharType.VARCHAR ) ) {
            return ( ( Slice ) value ).toStringUtf8();
        } else if ( type.equals( BooleanType.BOOLEAN ) ) {
            return (boolean) value;
        } else {
            throw new UnsupportedOperationException("Query Builder can't handle type: " + type);
        }
    }

    public void createIndex( String index, Map<String,Object> properties ){
        HashMap<String, Object> settings = new HashMap<String, Object>();
        if ( properties.containsKey( ElasticsearchConfig.SETTING_NUMBER_OF_SHARDS ) ){
            settings.put( ElasticsearchConfig.SETTING_NUMBER_OF_SHARDS,
                            properties.get( ElasticsearchConfig.SETTING_NUMBER_OF_SHARDS ) );
        } else {
            settings.put( ElasticsearchConfig.SETTING_NUMBER_OF_SHARDS, config.getShardsNum() );
        }

        if ( properties.containsKey( ElasticsearchConfig.SETTING_NUMBER_OF_RESPLICAS ) ){
            settings.put( ElasticsearchConfig.SETTING_NUMBER_OF_RESPLICAS,
                            properties.get( ElasticsearchConfig.SETTING_NUMBER_OF_RESPLICAS ) );
        } else {
            settings.put( ElasticsearchConfig.SETTING_NUMBER_OF_RESPLICAS, config.getReplicasNum() );
        }
        client.admin().indices().prepareCreate( index ).setSettings( settings ).execute().actionGet();
    }

    public void dropIndex( String index ){
        client.admin().indices().prepareDelete( index ).execute().actionGet();
    }

    public void createMapping( ConnectorTableMetadata tableMetadata ) throws IOException {
        List<ColumnMetadata> columns = tableMetadata.getColumns();
        Map<String,Object> properties = tableMetadata.getProperties();
        SchemaTableName schemaTableName = tableMetadata.getTable();
        Map<String,Map<String,Object>> source = getSource( columns, properties );
        XContentBuilder mappingBuilder = jsonBuilder();
        mappingBuilder.startObject().startObject( schemaTableName.getTableName() ).startObject("properties");

        Set<Map.Entry<String, Map<String,Object>>> set = source.entrySet();
        Iterator<Map.Entry<String, Map<String,Object>>> iterator = set.iterator();
        while( iterator.hasNext() ){
            Map.Entry<String,Map<String,Object>> e = iterator.next();
            mappingBuilder.startObject( e.getKey() );

            Map<String,Object> map = e.getValue();
            Iterator<String> kiterator = map.keySet().iterator();
            while ( kiterator.hasNext() ){
                String k = kiterator.next();
                mappingBuilder.field( k, map.get( k ) );
            }
            mappingBuilder.endObject();
        }
        mappingBuilder.endObject().endObject().endObject();

        PutMappingResponse response = this.client.admin().indices()
                                        .preparePutMapping(schemaTableName.getSchemaName())
                                        .setType(schemaTableName.getTableName())
                                        .setSource(mappingBuilder)
                                        .setIgnoreConflicts(true).execute().actionGet();
    }

    private Map<String,Map<String,Object>> getSource( List<ColumnMetadata> columns, Map<String,Object> properties ){
        ImmutableMap.Builder<String,Map<String,Object>> builder = ImmutableMap.builder();
        for ( ColumnMetadata columnMetadata : columns ){
            Map<String,Object> map = new HashMap<String,Object>();
            builder.put( columnMetadata.getName(), map );
            map.put( "type", getEsType( columnMetadata.getType() ) );
        }
        Map<String,Map<String,Object>> source = builder.build();
        Iterator<Map.Entry<String,Object>> iterator = properties.entrySet().iterator();
        while ( iterator.hasNext() ) {
            Map.Entry<String,Object> entry = iterator.next();
            String k = entry.getKey();
            String v = (String) entry.getValue();
            String[] cols = v.split(";");
            for ( String col : cols ){
                String[] pros = col.split(":");
                source.get( pros[0] ).put( k, pros[1] );
            }
        }
        return source;
    }

    private String getEsType( Type type ){
        if ( type instanceof BigintType ) {
            return "long";
        } else if ( type instanceof IntegerType ) {
            return "integer";
        } else if ( type instanceof DoubleType ) {
            return "double";
        } else if ( type instanceof VarcharType ) {
            return "string";
        } else if ( type instanceof BooleanType ) {
            return "boolean";
        } else {
            throw new UnsupportedOperationException("Query Builder can't handle type: " + type);
        }
    }

    public void dropMapping( ElasticsearchTableHandle th ) {
        client.admin().indices().prepareDeleteMapping( th.getSchemaName() ).setType( th.getTableName() ).execute().actionGet();
    }

    public BulkRequestBuilder getBulkRequest( ){
        return client.prepareBulk();
    }

    public IndexRequestBuilder getIndexBuilder( String index, String type, Map<String,Object> map ) throws IOException {
        return client.prepareIndex( index, type ).setSource( buildDoc( map ) );
    }

    private XContentBuilder buildDoc( Map<String, Object> doc ) throws IOException {
        XContentBuilder xContentBuilder = jsonBuilder().startObject();
        Iterator iterator = doc.entrySet().iterator();
        while(iterator.hasNext()){
            Map.Entry  entry=(Map.Entry)iterator.next();
            Object key=entry.getKey();
            Object value=entry.getValue();
            xContentBuilder.field((String)entry.getKey(),entry.getValue());
        }
        xContentBuilder.endObject();

        return  xContentBuilder;
    }
}
