package com.hy.bigdata.modules.spark.examples.session;

import org.apache.spark.sql.SparkSession;

/**
 *
 *  simple example of off-line sparkSession, when example is complex , it will be splitting
 *
 */
public class ISparkSession {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("default_session_test").getOrCreate();


    }

}
