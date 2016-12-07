package com.tscloud.presto.es.utils;

import com.facebook.presto.spi.type.Type;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

/**
 * Created by Administrator on 2016/11/16.
 */
public class TypeMapping {

    public static Type getType( String type ){
        Type prestoType ;
        switch (type) {
            case "double":
            case "float":
                prestoType = DOUBLE;
                break;
            case "integer":
                prestoType = INTEGER;
                break;
            case "long":
                prestoType = BIGINT;
                break;
            case "string":
                prestoType = VARCHAR;
                break;
            case "boolean":
                prestoType = BOOLEAN;
                break;
            case "binary":
                prestoType = VARBINARY;
                break;
            case "nested":
                prestoType = VARCHAR; //JSON
                break;
            default:
                prestoType = VARCHAR; //JSON
                break;
        }
        return prestoType;
    }

}
