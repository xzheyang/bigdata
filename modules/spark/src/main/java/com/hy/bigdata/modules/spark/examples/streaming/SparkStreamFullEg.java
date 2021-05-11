package com.hy.bigdata.modules.spark.examples.streaming;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkStreamFullEg {

    /**
     *     simple begin
     *     wordCount stream split by @param splitStr key in socket,and just show include 10s
     *     test method :
     *      in Windows, can use nc -L -p 8866 test the stream
     *      in linux, can use nc -lk 8866 test the stream
     *
     *
     * @param host          ip host
     * @param port          ip port
     * @param splitStr      use the string to split
     */
    public static void wordCountInSchedule(String host,int port,String splitStr) {

        if (StringUtils.isBlank(host) || splitStr == null ){
            throw new RuntimeException(" error blank params, host = ["+host+"] splitStr = ["+splitStr+"] ");
        }

        //create sparkStream
        SparkConf conf = new SparkConf().setAppName("socket-spark-stream").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaStreamingContext jsc = new JavaStreamingContext(sparkContext, Durations.seconds(10));

        //open socket
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream(host, port);

        // Split each line into words
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(splitStr)).iterator());
        // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(Integer::sum);

        // Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();

        jsc.start();              // Start the computation
        try {
            jsc.awaitTermination();   // Wait for the computation to terminate
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(" killed the wordCount spark streaming ");
        }

    }

}
