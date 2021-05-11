package com.hy.bigdata.modules.spark.examples.struct_stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

public class ISparkStructStream {


    /**
     *
     *  simple begin
     *     wordCount stream split by @param splitStr key in socket,and just show include 10s
     *     test method :
     *      in Windows, can use nc -L -p 9898 test the stream
     *      in linux, can use nc -lk 9898 test the stream
     *
     */
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("spark 官方样例");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        //读取socket
        // Create DataFrame representing the stream of input lines from connection to localhost:9898
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9898)
                .load();

        // Split the lines into words
        Dataset<String> words = lines
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

        // Generate running word count
        Dataset<Row> wordCounts = words.groupBy("value").count();

        // Start running the query that prints the running counts to the console
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
            throw new RuntimeException("样例终止");
        }




    }

}
