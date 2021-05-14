package com.hy.bigdata.modules.spark.examples.common.udf;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.hy.bigdata.modules.spark.api.SparkSessionTemplate;
import com.hy.bigdata.modules.spark.examples.common.udf.functions.DeleteFirstStrUdf;
import com.hy.bigdata.modules.spark.examples.common.udf.functions.DistinctStrUdf;
import com.hy.bigdata.modules.spark.examples.common.udf.functions.StrContainMapUdf;
import com.hy.bigdata.modules.spark.structure.utils.DatasetCreateUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 *
 *
 */
public class ISparkUdf {


    private final static SparkSessionTemplate SESSION_TEMPLATE = SparkSessionTemplate.getInstance();
    private final static SparkSession SESSION = SESSION_TEMPLATE.getSession();

    /**
     *  Spark关于自定义函数的使用
     *
     */
    public static void main(String[] args) {
        Dataset<Row> example = createExample();
        registerInSession(example);
        registerInCol(example);
    }


    /**
     *  全局注册udf函数
     *
     * @param example   样例数据
     */
    public static void registerInSession(Dataset<Row> example) {
        //1.注册和创建函数,不同session中似乎有区别
        SESSION.udf().register("distinctStrUdf", new DistinctStrUdf(), DataTypes.StringType);
        SESSION.udf().register("deleteFirstStrUdf", new DeleteFirstStrUdf(), DataTypes.StringType);
        SESSION.udf().register("strContainMapUdf",new StrContainMapUdf(),DataTypes.StringType);

        //2.在spark sql中使用
        //2.1 简单参数使用
        example.selectExpr(" id","explode(split(distinctStrUdf(content,','),',')) as content_dis ").show();
        example.selectExpr(" id","explode(split(deleteFirstStrUdf(content,','),',')) as content_rep ").
                filter((FilterFunction<Row>)  row->StringUtils.isNotBlank(row.getAs("content_rep"))).show();

        //2.2 map等参数使用
        ImmutableMap<String, String> javaMap = ImmutableMap.<String, String>builder().put("test", "1").put("ww", "2").build();
        String mapSql =  "map("+Joiner.on(",").join(javaMap.keySet().stream().flatMap(k -> Arrays.stream(new String[]{k, javaMap.get(k)})).map(s -> "'" + s + "'").toArray(String[]::new))+")";
        example.selectExpr("*","strContainMapUdf(content,',',"+mapSql+")").show();

    }


    /**
     *  生产udf函数,对col使用
     *
     * @param example   样例数据
     */
    public static void registerInCol(Dataset<Row> example) {
        //1.生成自定义函数(不是在session中注册不能在spark sql中使用)
        UserDefinedFunction distinctStrUdf = functions.udf(new DistinctStrUdf(), DataTypes.StringType);
        UserDefinedFunction strContainMapUdf = functions.udf(new StrContainMapUdf(), DataTypes.StringType);

        //2.在column中使用(注意参数数量不对时报错是转换函数失败)
        //2.1 简单str使用
        example.select(functions.col("id"),distinctStrUdf.apply(functions.col("content"),functions.lit(",")).alias("content_dis")).show();

        //2.2 传入map使用
        ImmutableMap<String, String> javaMap = ImmutableMap.<String, String>builder().put("test", "1").put("ww", "2").build();
        //2.2之前版本转换(但是functions.typedLit的2.2版本转换java暂时不会使用)
        Column dicMap = functions.map(javaMap.keySet().stream().flatMap(k -> Arrays.stream(new String[]{k, javaMap.get(k)})).map(functions::lit).toArray(Column[]::new));
        example.withColumn("dict",strContainMapUdf.apply(
                functions.col("content"),functions.lit(","), dicMap )
        ).show();


    }


    /**
     * @return  生成测试样例
     */
    public static Dataset<Row> createExample() {

        List<Row> list = new ArrayList<>();
        list.add(RowFactory.create("1","you","test,ww,test"));
        list.add(RowFactory.create("2","me","test,yy,mm"));
        list.add(RowFactory.create("3","her",null));

        return DatasetCreateUtils.quickCreateStrDs(list, Lists.newArrayList("id","name","content"));
    }


}
