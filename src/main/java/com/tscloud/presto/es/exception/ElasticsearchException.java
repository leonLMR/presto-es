package com.tscloud.presto.es.exception;

/**
 * Created by Administrator on 2016/11/16.
 */
public class ElasticsearchException extends RuntimeException {
    public ElasticsearchException( Throwable e ){
        super( e );
    }
    public ElasticsearchException( String msg, Throwable e ){
        super( msg, e );
    }
}
