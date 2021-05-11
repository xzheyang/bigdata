package com.hy.bigdata.modules.spark.examples.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 *
 * spark streaming
 *
 */
public class SparkStreamProcess {


    public static void main(String[] args) {
        JavaStreamingContext simpleStream = checkPointAndRecover("./tmp/SparkStreamProcess",()->{
            JavaStreamingContext jsc = createSimple("createSimpleStream", Durations.seconds(10));
            JavaReceiverInputDStream<String> socketTextStream = jsc.socketTextStream("localhost", 8866, StorageLevel.MEMORY_AND_DISK());
            countSimple(socketTextStream);
            return jsc;
        });
        startSimple(simpleStream);
    }


    /**
     *  create javaStreamingContext
     *  这个示例只计算秒数内的数据,不会管以前的数据
     *
     * 一.在定义一个 context 之后,您必须执行以下操作。
     *
     * 通过创建输入 DStreams 来定义输入源。
     * 通过应用转换和输出操作 DStreams 定义流计算（streaming computations）。
     * 开始接收输入并且使用 streamingContext.start() 来处理数据。
     * 使用 streamingContext.awaitTermination() 等待处理被终止（手动或者由于任何错误）。
     * 使用 streamingContext.stop() 来手动的停止处理。
     *
     * 二.需要记住的几点:
     * 一旦一个 context 已经启动，将不会有新的数据流的计算可以被创建或者添加到它。
     * 一旦一个 context 已经停止，它不会被重新启动。
     * 同一时间内在 JVM 中只有一个 StreamingContext 可以被激活。
     * 在 StreamingContext 上的 stop() 同样也停止了 SparkContext。为了只停止 StreamingContext，设置 stop() 的可选参数，名叫 stopSparkContext 为 false。
     * 一个 SparkContext 就可以被重用以创建多个 StreamingContexts，只要前一个 StreamingContext 在下一个StreamingContext 被创建之前停止（不停止 SparkContext）。
     *
     */
    public static JavaStreamingContext createSimple(String appStreamName, Duration duration) {

        SparkConf sparkConf = new SparkConf().
                setAppName(appStreamName).
                setMaster("local[*]")           // in special, local[cpu cores] ,eg: local[1]  meaning use one cpu core
                ;

        return new JavaStreamingContext(sparkConf,duration);   // in one second compute once, and not compute before

    }


    /**
     *  start javaStreamingContext
     *
     * @param javaStreamingContext  input javaStream
     */
    public static void startSimple(JavaStreamingContext javaStreamingContext) {

        javaStreamingContext.start();
        try {
            javaStreamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(" killed the wordCount spark streaming ");
        }

    }

    /**
     * simple compute count split by space key
     *
     * @param dStream example stream
     */
    public static JavaPairDStream<String, Integer> countSimple(JavaDStream<String> dStream) {

        JavaDStream<String> lines = dStream.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairDStream = lines.mapToPair(l -> new Tuple2<>(l, 1));
        JavaPairDStream<String, Integer> wordCountDStream = pairDStream.reduceByKey(Integer::sum);

        wordCountDStream.print();

        return wordCountDStream;
    }


    /**
     *   设置容错检查点,且恢复数据
     *
     *
     * question:
     *  1.若流式程序代码或配置改变，则先停掉旧的spark Streaming程序，然后把新的程序打包编译后重新执行,会导致反序列失败报错,
     *      这样像kafka就需要维护消费的 offsets,保证容错
     *
     *  2.spark在使用checkpoint恢复的时候不能再执行流的定义的流程，新加入的流的状态在恢复完成后的spark状态下处于未初始化状态，
     *      在spark根据checkpoint恢复的时候将不会再对各个流进行初始化，而是直接保存的状态中恢复。
     *      这将导致新加入的流还未初始化就被调用，抛出stream还未初始化的异常。
     *
     *  3.spark在使用checkpoint恢复的过程中，不能恢复kryo序列化的类（比如采用kryo序列化的广播变量）。
     *      在进行checkpoint的过程，直接使用jdk的ObjectOutputStream进行序列化，如果只是实现了kryo序列化接口的类是不能被成功序列化的，自然是无法被写进checkpoint文件中被恢复的。
     *
     * @param checkpointPath    path
     * @param createFunction    create function
     * @return  checkpoint data recover, if path not have checkpoint,create a new by create function
     */
    public static JavaStreamingContext checkPointAndRecover(String checkpointPath, Function0<JavaStreamingContext> createFunction) {
        JavaStreamingContext recoverContext = JavaStreamingContext.getOrCreate(checkpointPath, createFunction);  //attention 好像这么做不写函数里会初始化失败
        recoverContext.checkpoint(checkpointPath);
        return recoverContext;
    }


}
