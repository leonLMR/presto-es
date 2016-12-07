package com.tscloud.presto.es.utils;

import java.util.Locale;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Created by Administrator on 2016/11/16.
 */
public class ClassCastUtil {
    public static <A, B extends A> B checkType( A value, Class<B> target, String name ) {
        requireNonNull(value, String.format(Locale.ENGLISH, "%s is null", name));
        checkArgument(target.isInstance(value),
                "%s must be of type %s, not %s",
                name,
                target.getName(),
                value.getClass().getName());
        return target.cast(value);
    }
}
