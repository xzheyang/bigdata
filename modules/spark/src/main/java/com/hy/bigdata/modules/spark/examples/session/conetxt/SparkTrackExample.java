package com.hy.bigdata.modules.spark.examples.session.conetxt;

import com.google.common.collect.Lists;
import com.hy.bigdata.modules.spark.api.SparkSessionTemplate;
import com.hy.bigdata.modules.spark.structure.utils.DatasetCreateUtils;
import org.apache.spark.SparkJobInfo;
import org.apache.spark.SparkStageInfo;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkStatusTracker;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import scala.Function1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author yang.he
 * @date 2021-05-18 18:56
 *
 *      spark堆栈案例
 *
 */
public class SparkTrackExample {

    public static void main(String[] args) {

        SparkSessionTemplate sessionTemplate = SparkSessionTemplate.getInstance();
        SparkSession session = sessionTemplate.getSession();
        JavaSparkContext context = sessionTemplate.getContext();

        session.createDataset(Arrays.asList("a","b","c","d","e"), Encoders.STRING()).repartition(5)
                .map(new IdentityWithDelay<>(),Encoders.STRING()).
                foreach(s -> {
                    Thread.sleep(2*100);    //测试函数的1/10
                    statusTrackerPrinter();
                });



    }


    /**
     *      打印tracker数据
     */
    public static void statusTrackerPrinter() {
        SparkSessionTemplate sessionTemplate = SparkSessionTemplate.getInstance();
        JavaSparkContext context = sessionTemplate.getContext();

        JavaSparkStatusTracker tracker = context.statusTracker();
        int[] activeJobIds = tracker.getActiveJobIds();
        int[] activeStageIds = tracker.getActiveStageIds();

        for (int activeJobId : activeJobIds) {
            SparkJobInfo jobInfo = tracker.getJobInfo(activeJobId);
            int[] stageIds = jobInfo.stageIds();
            for (int stageId : stageIds) {
                SparkStageInfo stageInfo = tracker.getStageInfo(stageId);
                int attemptId = stageInfo.currentAttemptId();
                int numTasks = stageInfo.numTasks();
                int numActiveTasks = stageInfo.numActiveTasks();
                int numFailedTasks = stageInfo.numFailedTasks();
                int numCompletedTasks = stageInfo.numCompletedTasks();
                System.out.println("当前jobId = [" + activeJobId + "],stageId =[" + stageId + "],attemptId=[" + attemptId + "],numTasks=[" + numTasks + "],numActiveTasks=[" + numActiveTasks + "],numFailedTasks=[" + numFailedTasks + "],numCompletedTasks=[" + numCompletedTasks + "]");
            }
        }
    }

    /**
     *  延迟测试函数
     *
     * @param <T>
     */
    public static final class IdentityWithDelay<T> implements MapFunction<T, T> {
        @Override
        public T call(T x) throws Exception {
            Thread.sleep(2 * 1000);  // 2 seconds
            return x;
        }
    }



}
