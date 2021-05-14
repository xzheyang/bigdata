package com.hy.bigdata.modules.spark.api;

import com.hy.bigdata.modules.common.environment.ConfigEnvContext;
import com.hy.bigdata.modules.spark.common.javaheap.JavaHeapUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 *  @author yang.he
 *  @date 2021-04-10 10:53
 *
 *    SparkSession的总框架模板入口,由此指引整合计算框架
 *
 *    expect:
 *       1.全局安全唯一的环境
 *       2.血缘关系/调用链维护
 *       3.统一元数据关系维护
 *       4.封装算子/组件的操作
 *       5.提供安全便捷的ETL操作
 *       6.性能优化
 *       7.业务和组件操作去耦合
 *       8.流批一体
 *
 *
 */
public class SparkSessionTemplate {


    /**
     * 整体统一的配置和重要context
     *
     */
    private SparkConf conf;
    private SparkSession session;
    private JavaSparkContext context;
    private SparkContext sparkContext;

    /**
     * singleton
     */
    private static SparkSessionTemplate instance;


    //singleton construct

    private SparkSessionTemplate(){  init(); }
    public static SparkSessionTemplate getInstance() {
        if(instance==null){
            synchronized (SparkSessionTemplate.class){
                if (instance==null){
                    instance = new SparkSessionTemplate();
                    return instance;
                }
            }
        }
        return instance;
    }


    /**
     * real init sessionTemplate
     */
    private void init(){
        init(ConfigEnvContext.getStringByFilePath("template/spark","com.spark.master"),
                JavaHeapUtils.getInvokeName(false),
                Boolean.parseBoolean(ConfigEnvContext.getStringByFilePath("template/spark","com.spark.enableHive")));
    }


    /**
     *  detail init sessionTemplate
     *
     * @param master        主机
     * @param name          appName
     * @param enableHive    是否开启hive
     */
    private void init(String master,String name,boolean enableHive){
        //这样配置,本质是用的一个Context
        conf = new SparkConf().setAppName(name).setMaster(master);
        conf = setConf(conf);
        sparkContext = new SparkContext(conf);
        context = new JavaSparkContext(sparkContext);
        SparkSession.Builder builder = new SparkSession.Builder().sparkContext(sparkContext);
        if (enableHive){
            session = builder.enableHiveSupport().getOrCreate();
        }else {
            session = builder.getOrCreate();
        }
    }



    /**
     * SparkConf配置内部属性
     *
     * @param conf 未配置默认属性的conf
     * @return 配置好的SparkConf
     */
    private static SparkConf setConf(SparkConf conf){
        return conf.set("org.apache.spark.debug.maxToStringFields", "100")
                .set("org.apache.spark.sql.crossJoin.enabled", "true")
                .set("org.apache.spark.kryoserializer.buffer","64m")
                .set("org.apache.spark.serializer","org.apache.org.apache.spark.serializer.KryoSerializer")
                .set("org.apache.spark.scheduler.listenerbus.eventqueue.size","100000")
                .set("org.apache.spark.sql.autoBroadcastJoinThreshold","-1")
                .set("org.apache.spark.default.parallelism","30")
                .set("org.apache.spark.driver.maxResultSize","8g")
//                .set("org.apache.spark.kryo.registrator", "com.sinitek.dc.fxq_235_poc.module.common.data.tool.CommonParametersKryoRegistrator")
                .set("org.apache.spark.kryoserializer.buffer","64m")
                .set("org.apache.spark.kryoserializer.buffer.max","768m")
                ;
    }


    /**
     * @return  获得sparkConf,如果没有会创建实例
     */
    public SparkConf getConf() {
        if(conf ==null){
            getInstance();
        }
        return conf;
    }

    /**
     * @return  获得SparkSession,如果没有会创建实例
     */
    public SparkSession getSession() {
        if(session ==null){
            getInstance();
        }
        return session;
    }

    /**
     * @return  获得JavaSparkContext,如果没有会创建实例
     */
    public JavaSparkContext getContext() {
        if (context == null){
            getInstance();
        }
        return context;
    }

    /**
     * @return  获得SparkContext,如果没有会创建实例
     */
    public SparkContext getSparkContext() {
        if (sparkContext == null){
            getInstance();
        }
        return sparkContext;
    }


}
