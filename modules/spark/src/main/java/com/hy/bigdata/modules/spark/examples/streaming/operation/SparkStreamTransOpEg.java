package com.hy.bigdata.modules.spark.examples.streaming.operation;

import com.hy.bigdata.modules.spark.examples.streaming.SparkStreamProcess;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SparkStreamTransOpEg {

    public static void main(String[] args) {

        //must checkpoint and best to need recover
        JavaStreamingContext context = SparkStreamProcess.checkPointAndRecover( "./tmp/SparkStreamTransOpEg",()->{
            JavaStreamingContext jsc = SparkStreamProcess.createSimple("createSimpleStream", Durations.seconds(5));
            JavaReceiverInputDStream<String> socketTextStream = jsc.socketTextStream("localhost", 8866, StorageLevel.MEMORY_AND_DISK());
            JavaPairDStream<String, Integer> pairDStream = SparkStreamProcess.countSimple(socketTextStream);
            updateStateByKey(jsc,pairDStream);  //额外观察到在stream中两个action算子会互相影响,这里是前面的未生效
            return jsc;
        });
        //run
        SparkStreamProcess.startSimple(context);

    }

    /**
     *
     *  通过获取初始状态和现有状态获得结果
     *
     * @param jsc               spark stream context
     * @param wordsStream       the latest calculations
     */
    public static void updateStateByKey(JavaStreamingContext jsc, JavaPairDStream<String, Integer> wordsStream) {


        // Initial state RDD input to mapWithState
        List<Tuple2<String, Integer>> tuples = Arrays.asList(new Tuple2<>("hello", 1), new Tuple2<>("world", 1));
        JavaPairRDD<String, Integer> initialRDD = jsc.sparkContext().parallelizePairs(tuples);

        // Update the cumulative count function
        Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc =
                (word, one, state) -> {
                    int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
                    Tuple2<String, Integer> output = new Tuple2<>(word, sum);
                    state.update(sum);
                    return output;
                };

        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateStream =
                wordsStream.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD));

        stateStream.checkpoint(Durations.seconds(5));   //checkpoint 会影响性能, DStream 的5到10个滑动间隔的 checkpoint 间隔

        stateStream.print();

    }

}
