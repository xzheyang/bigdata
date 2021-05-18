package com.hy.bigdata.modules.spark.examples.common.functions;

import com.hy.bigdata.modules.spark.structure.utils.DatasetCreateUtils;
import com.hy.bigdata.modules.spark.structure.utils.SchemaUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yang.he
 * @date 2021-05-11 18:41
 *
 *  spark内置函数案例
 *
 */
public class SparkBuiltInFuncExample {

    public static void main(String[] args) {
        staticColOperate();
        arithmeticOperate();
        windowFunctions();
    }

    /**
     *  静态列操作
     *
     */
    public static void staticColOperate() {
        Dataset<Row> example = createExample();

        //添加静态列
        example.withColumn("status",functions.lit(1)).show();
        //添加一列时间列
        example.select(example.col("*"),functions.current_date().alias("date")).show();
    }

    /**
     *   算术操作
     */
    public static void arithmeticOperate() {
        Dataset<Row> example = createExample();

        //算术
        //除3并且限制两位小数(func和expr的方式)
        example.select(example.col("name"), example.col("number").divide(3).cast(DataTypes.createDecimalType(38,2)).alias("div")).show();
        example.selectExpr("name","cast(number/3 as decimal(38,2)) as div").show();
    }


    /**
     * 窗口函数
     *
     * 类别	    SQL	            DataFrame	    含义
     * 排名函数	rank	        rank	        为相同组的数据计算排名，如果相同组中排序字段相同，当前行的排名值和前一行相同；如果相同组中排序字段不同，则当前行的排名值为该行在当前组中的行号；因此排名序列会出现间隙
     * 排名函数	dense_rank	    denseRank	    为相同组内数据计算排名，如果相同组中排序字段相同，当前行的排名值和前一行相同；如果相同组中排序字段不同，则当前行的排名值为前一行排名值加1；排名序列不会出现间隙
     * 排名函数	percent_rank	percentRank	    该值的计算公式(组内排名-1)/(组内行数-1)，如果组内只有1行，则结果为0
     * 排名函数	ntile	        ntile	        将组内数据排序然后按照指定的n切分成n个桶，该值为当前行的桶号(桶号从1开始)
     * 排名函数	row_number	    rowNumber	    将组内数据排序后，该值为当前行在当前组内的从1开始的递增的唯一序号值
     * 分析函数	cume_dist	    cumeDist	    该值的计算公式为：组内小于等于当前行值的行数/组内总行数
     * 分析函数	lag	            lag	            用法：lag(input, [offset, [default]]),计算组内当前行按照排序字段排序的之前offset行的input列的值，如果offset大于当前窗口(组内当前行之前行数)则返回default值，default值默认为null
     * 分析函数	lead	        lead	        用法：lead(input, [offset, [default]])，计算组内当前行按照排序字段排序的之后offset行的input列的值，如果offset大于当前窗口(组内当前行之后行数)则返回default值，default值默认为null
     * 聚合函数  avg,sum,max..                   用法: 聚合函数计算 注:如果这里用了 order by 这里是排名计算,只算当前1位 eg:第一位的平均值怎么都是自己,后面是前面+自己的平均值,没有则正常计算,但是有误差
     */
    public static void windowFunctions() {
        Dataset<Row> example = createExample();

        //根据id分组并且按照number排名(相同分数为1个排名,且后面名次不会加位数)
        example.selectExpr("*", " dense_rank() OVER (PARTITION BY id ORDER BY number DESC) as rank ").show();
        example.select(functions.col("*"),functions.dense_rank().over(Window.partitionBy("id").orderBy(functions.col("number").desc())).alias("rank")).show();

        //根据id分组并且按照number排名,且找同组内number-number平均值的差值[有精确值的误差]  注: 使用( number - avg(number) OVER(PARTITION BY id ORDER BY number DESC ) ) as number_avg_diff 是错误的
        example.selectExpr("*"," ( number - avg(number) OVER(PARTITION BY id) ) as number_avg_diff").show();
        example.select(functions.col("*"),functions.col("number").minus(
                functions.avg("number").over(Window.partitionBy("id")))
        .alias("number_avg_diff")).show();


    }





    /**
     * @return  生成测试样例
     */
    public static Dataset<Row> createExample() {

        List<Row> list = new ArrayList<>();
        list.add(RowFactory.create("1","hy","who",1.2));
        list.add(RowFactory.create("1","her","who",2.3));
        list.add(RowFactory.create("1","w","wu",2.3));
        list.add(RowFactory.create("2","cx","why",1.0));
        list.add(RowFactory.create("3","yh",null,0.3));
        list.add(RowFactory.create("5","better",null,1.3));

        StructType schema = SchemaUtils.crateStrSchema("id", "name", "content").
                add("number", DataTypes.DoubleType, true);

        return DatasetCreateUtils.quickCreateStrDs(list, schema);
    }


}
