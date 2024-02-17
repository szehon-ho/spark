package org.apache.spark.sql.connector.catalog.functions;

import org.apache.spark.annotation.Evolving;

/**
 * A 'reduction' function for user-defined functions.
 *
 * This means that given a user_defined function f_source(x) that is 'reducible' on
 * another '' user_defined function f_target(x),
 * a reduction function r(x) exists such that r(f_source(x)) = f_target(x) for all input x.
 * @param <T> function output type
 * @since 4.0.0
 */
@Evolving
public interface Reducer<T> {
    T reduce(T arg1);
}
