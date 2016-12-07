package com.tscloud.presto.es.test;

import com.alibaba.fastjson.JSON;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.tscloud.presto.es.ElasticsearchPlugin;
import com.tscloud.presto.es.ElasticsearchTableHandle;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2016/11/16.
 */
public class ElasticsearchTest {

    public static void main( String args[] ) {
        ElasticsearchPlugin plugin = new ElasticsearchPlugin();
        Iterable<ConnectorFactory> iterable = plugin.getConnectorFactories();
        ConnectorFactory connectorFactory = iterable.iterator().next();

        Connector connector= connectorFactory.create("test",getConfig(),null);
        //getSchemas( connector );
        //getTable( connector, "yl_index", "dddd" );
        //getTables( connector, "yl_index" );
        //getTables( connector, null );
        listTableColumns(connector, "yl_index", "dddd");
    }

    private static Map<String,String> getConfig(){
        Map<String,String> map = new HashMap<String,String>();
        map.put("cluster.name","truecloud_db_development");
        map.put("client.hosts","172.192.100.52:9300");
        return map;
    }

    private static void getSchemas( Connector connector ){
        List<String> list = connector.getMetadata(null).listSchemaNames( null );
        for ( String str : list ){
            System.out.println( str );
        }
    }

    private static void getTable( Connector connector, String schema, String table ){
        ElasticsearchTableHandle tableHandle = new ElasticsearchTableHandle( schema, table );
        ConnectorTableMetadata metadata = connector.getMetadata( null ).getTableMetadata( null, tableHandle );
        System.out.println( JSON.toJSONString( metadata.getColumns() ) );
    }

    private static void getTables( Connector connector, String schema ){
        List list = connector.getMetadata( null ).listTables( null, schema );
        System.out.println( JSON.toJSONString( list ) );
    }

    private static void listTableColumns( Connector connector, String schema, String table ){
        SchemaTablePrefix stp = new SchemaTablePrefix( schema, table );
        SchemaTableName st = new SchemaTableName( schema, table );
        JSON.toJSONString( connector.getMetadata(null).listTableColumns(null, stp).get( st ) );
    }


}
