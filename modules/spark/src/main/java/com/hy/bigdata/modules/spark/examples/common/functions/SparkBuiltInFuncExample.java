package com.hy.bigdata.modules.spark.examples.common.functions;

import com.hy.bigdata.modules.spark.structure.utils.DatasetCreateUtils;
import com.hy.bigdata.modules.spark.structure.utils.SchemaUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
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

        Dataset<Row> example = createExample();

        //添加静态列
        example.withColumn("status",functions.lit(1)).show();
        //添加一列时间列
        example.select(example.col("*"),functions.current_date().alias("date")).show();

        //算术
        //除3并且限制两位小数(func和expr的方式)
        example.select(example.col("name"), example.col("number").divide(3).cast(DataTypes.createDecimalType(38,2)).alias("div")).show();
        example.selectExpr("name","cast(number/3 as decimal(38,2)) as div").show();


    }


    /**
     * @return  生成测试样例
     */
    public static Dataset<Row> createExample() {

        List<Row> list = new ArrayList<>();
        list.add(RowFactory.create("1","hy","who",1.2));
        list.add(RowFactory.create("2","cx","why",1.0));
        list.add(RowFactory.create("3","yh",null,0.3));
        list.add(RowFactory.create("5","better",null,1.3));

        StructType schema = SchemaUtils.crateStrSchema("id", "name", "content").
                add("number", DataTypes.DoubleType, true);

        return DatasetCreateUtils.quickCreateStrDs(list, schema);
    }


}
