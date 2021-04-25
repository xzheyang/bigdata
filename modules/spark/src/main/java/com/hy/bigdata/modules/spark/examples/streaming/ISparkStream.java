package com.hy.bigdata.modules.spark.examples.streaming;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

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
