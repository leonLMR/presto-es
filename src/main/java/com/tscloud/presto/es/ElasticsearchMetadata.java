package com.tscloud.presto.es;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.tscloud.presto.es.exception.ElasticsearchException;
import com.tscloud.presto.es.utils.ClassCastUtil;
import io.airlift.slice.Slice;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;

import static java.util.Objects.requireNonNull;


/**
 * Created by Administrator on 2016/11/16.
 */
public class ElasticsearchMetadata implements ConnectorMetadata {

    private ElasticsearchClient client;

    @Inject
    public ElasticsearchMetadata( ElasticsearchClient client ){
        this.client = client;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession connectorSession) {
        return Arrays.asList( client.listIndexs() );
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession connectorSession, SchemaTableName schemaTableName) {
        try{
            ElasticsearchTable table = client.getTable( schemaTableName.getSchemaName(), schemaTableName.getTableName() );
            if ( table != null ){
                return new ElasticsearchTableHandle( schemaTableName.getSchemaName(), schemaTableName.getTableName() );
            } else {
                return null;
            }
        } catch ( IOException e ){
            throw new ElasticsearchException( e );
        }
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession connectorSession, ConnectorTableHandle connectorTableHandle,
                                                            Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> optional) {
        ElasticsearchTableHandle tableHandle = (ElasticsearchTableHandle) connectorTableHandle;
        ConnectorTableLayout layout = new ConnectorTableLayout( new ElasticsearchTableLayoutHandle( tableHandle, constraint.getSummary() ) );

        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession connectorSession, ConnectorTableLayoutHandle connectorTableLayoutHandle) {
        return new ConnectorTableLayout( connectorTableLayoutHandle );
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession connectorSession, ConnectorTableHandle connectorTableHandle) {
        ElasticsearchTableHandle tableHandle = (ElasticsearchTableHandle) connectorTableHandle;
        ElasticsearchTable table = null;
        try {
            table = client.getTable( tableHandle.getSchemaName(), tableHandle.getTableName() );
        } catch (IOException e) {
            throw new ElasticsearchException( " get table error. ", e );
        }
        List<ColumnMetadata> columnMetadatas = new ArrayList<ColumnMetadata>();
        for ( ElasticsearchColumn column : table.getColumns() ){
            columnMetadatas.add( new ColumnMetadata( column.getName(), column.getType() ) );
        }
        return new ConnectorTableMetadata( new SchemaTableName( table.getSchema(), table.getName() ), columnMetadatas );
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession connectorSession, String schema) {
        return client.getTables( schema );
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession connectorSession, ConnectorTableHandle connectorTableHandle) {
        ElasticsearchTableHandle tableHandle = (ElasticsearchTableHandle) connectorTableHandle;
        ElasticsearchTable table = null;
        try {
            table = client.getTable( tableHandle.getSchemaName(), tableHandle.getTableName() );
        } catch (IOException e) {
            throw new ElasticsearchException( " get table error. ", e );
        }
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        List<ElasticsearchColumn> columns = table.getColumns();
        for ( int index = 0; index < columns.size() ; index++ ){
            ElasticsearchColumn column = columns.get( index );
            ElasticsearchColumnHandle columnHandle = new ElasticsearchColumnHandle( column.getName(),
                                                column.getType(), column.getEsName(), column.getEsType(), index );
            columnHandles.put( column.getName(), columnHandle );
        }
        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession connectorSession, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        ElasticsearchTableHandle th = ClassCastUtil.checkType( tableHandle, ElasticsearchTableHandle.class, "tableHandle" );
        ElasticsearchColumnHandle ech = ClassCastUtil.checkType( columnHandle, ElasticsearchColumnHandle.class, "columnHandle" );
        ElasticsearchTable table = null;
        try {
            table = client.getTable( th.getSchemaName(), th.getTableName() );
        } catch (IOException e) {
            throw new ElasticsearchException( e );
        }

        if ( table == null ) {
            return null;
        }

        List<ElasticsearchColumn> columns = table.getColumns();
        for ( ElasticsearchColumn column : columns ){
            if ( column.getName().equals( ech.getName() ) ){
                return new ColumnMetadata( column.getName(), column.getType() );
            }
        }
        return new ColumnMetadata( ech.getName(), ech.getType() );
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession connectorSession, SchemaTablePrefix schemaTablePrefix) {
        requireNonNull(schemaTablePrefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        try {
            return client.getTableWithColumns(schemaTablePrefix.getSchemaName(), schemaTablePrefix.getTableName());
        } catch (IOException e) {
            throw new ElasticsearchException( e );
        }
    }

    @Override
    public void createSchema( ConnectorSession session, String schemaName, Map<String,Object> properties ) {
        client.createIndex(schemaName, properties);
    }

    @Override
    public void dropSchema( ConnectorSession session, String schemaName ) {
        client.dropIndex( schemaName );
    }

    @Override
    public void createTable( ConnectorSession session, ConnectorTableMetadata tableMetadata ) {
        try {
            client.createMapping( tableMetadata );
        } catch (IOException e) {
            throw new ElasticsearchException( e );
        }
    }

    @Override
    public void dropTable( ConnectorSession session, ConnectorTableHandle tableHandle) {
        ElasticsearchTableHandle th = ClassCastUtil.checkType( tableHandle, ElasticsearchTableHandle.class, "tableHandle" );
        client.dropMapping( th );
    }

    @Override
    public ConnectorInsertTableHandle beginInsert( ConnectorSession session, ConnectorTableHandle tableHandle ) {
        ElasticsearchTableHandle th = ClassCastUtil.checkType( tableHandle, ElasticsearchTableHandle.class, "tableHandle" );
        try {
            ElasticsearchTable et = client.getTable( th.getSchemaName(), th.getTableName() );
            return new ElasticsearchInsertTableHandle( et.getSchema(), et.getName(), et.getColumns() );
        } catch (IOException e) {
            throw new ElasticsearchException( e );
        }
    }


    @Override
    public void finishInsert( ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments ) {
        /*ElasticsearchInsertTableHandle insertTableHandle = ClassCastUtil.checkType( insertHandle,
                                                    ElasticsearchInsertTableHandle.class, "insertTableHandle" );*/

    }

}
