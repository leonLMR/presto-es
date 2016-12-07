package com.tscloud.presto.es.exception;

/**
 * Created by Administrator on 2016/11/15.
 */
public class ElasticsearchConfigException extends RuntimeException {
    public ElasticsearchConfigException( String msg ){
        super(msg);
    }
}
