package com.hy.bigdata.modules.spark.examples.streaming;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Queue;

/**
 *
 *  Spark Streaming Receivers define
 *
 *      Basic Sources:  File Streams,socket connections,Streams based on Custom Receivers,Queue of RDDs as a Stream
 *      Advanced Sources:   Advanced Sources
 *
 *      Receiver Reliability（接收器的可靠性）
 *      可以有两种基于他们的 reliability可靠性 的数据源。数据源（如 Kafka 和 Flume）允许传输的数据被确认。如果系统从这些可靠的数据来源接收数据，并且被确认（acknowledges）正确地接收数据，它可以确保数据不会因为任何类型的失败而导致数据丢失。这样就出现了 2 种接收器（receivers）:
 *
 *      Reliable Receiver（可靠的接收器） - 当数据被接收并存储在 Spark 中并带有备份副本时，一个可靠的接收器（reliable receiver）正确地发送确认（acknowledgment）给一个可靠的数据源（reliable source）。
 *      Unreliable Receiver（不可靠的接收器） - 一个不可靠的接收器（unreliable receiver）不发送确认（acknowledgment）到数据源。这可以用于不支持确认的数据源，或者甚至是可靠的数据源当你不想或者不需要进行复杂的确认的时候。
 *
 */
public class SparkStreamReceivers {

    public static void main(String[] args) {

        JavaStreamingContext jsc = SparkStreamProcess.createSimple("createSimpleStream", Durations.seconds(5));
        JavaDStream<String> textFileDStream = receiveFiles(jsc, "C:\\Users\\core\\Desktop\\test");
        SparkStreamProcess.countSimple(textFileDStream);
        SparkStreamProcess.startSimple(jsc);

    }


    /**
     *  receive textFileStream
     *  只会获得监听后来文件的数据,修改的数据也不会监听
     *
     * @param context   context
     * @param filePath  listen files-path
     *
     */
    public static JavaDStream<String> receiveFiles(JavaStreamingContext context,String filePath) {
        return context.textFileStream(filePath);
    }

    /**
     *  receive socket text
     *
     * @param context   stream context
     * @param host      receive socket host
     * @param port      receive socket port
     */
    public static JavaDStream<String> receiveSocket(JavaStreamingContext context,String host,int port) {
       return context.socketTextStream(host,port);
    }


    /**
     *  创建一个基于 RDDs 队列的 DStream，每个进入队列的 RDD 都将被视为 DStream 中的一个批次数据，并且就像一个流进行处理
     *  一般用于测试
     *
     * @param context  context
     * @param queue    queue of javaRDD
     *
     */
    public static JavaDStream<String> receiveQueueRDD(JavaStreamingContext context, Queue<JavaRDD<String>> queue) {
        return context.queueStream(queue);
    }


    /**
     *  用户基础自定义接收器
     *  Streams based on Custom Receivers
     *
     */
    public static JavaDStream<String> customBaseReceivers(JavaStreamingContext context) {
        return null;
    }


    public static void kafkaReceivers() {
    }

    public static void flumeReceivers() {
    }

    public static void kinesisReceivers() {
    }

}
