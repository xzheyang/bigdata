package com.hy.bigdata.modules.spark.streaming;

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

/**
 *
 *  simple example of online sparkStreaming, when example is complex , it will be splitting
 *
 */
public class ISparkStream {


    public static void main(String[] args) {
        JavaStreamingContext jsc = SparkStreamProcess.createSimple("createSimpleStream", Durations.seconds(5));
        JavaDStream<String> textFileDStream = SparkStreamReceivers.receiveFiles(jsc, "C:\\Users\\core\\Desktop\\test");
        SparkStreamProcess.countSimple(textFileDStream);
        SparkStreamProcess.startSimple(jsc);


    }





    /**
     *
     *
     *
     */
    public static void faultTolerant() {

    }



}
