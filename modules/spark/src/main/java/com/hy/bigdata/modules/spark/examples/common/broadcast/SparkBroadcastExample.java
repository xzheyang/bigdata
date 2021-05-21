package com.hy.bigdata.modules.spark.examples.common.broadcast;

import com.hy.bigdata.modules.spark.structure.utils.DatasetCreateUtils;
import com.hy.bigdata.modules.spark.structure.utils.SchemaUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yang.he
 * @date 2021-05-20 19:00
 *
 *  spark的broadcast内部原理和使用
 *
 */
public class SparkBroadcastExample {


    public static void main(String[] args) {

        Tuple2<Dataset<Row>, Dataset<Row>> example = createExample();

        //左反连接,用广播变量从传递
        example._1.join(functions.broadcast(example._2),functions.col("id"),"left_anti").show();

    }



    /**
     * @return  生成测试样例
     */
    public static Tuple2<Dataset<Row>,Dataset<Row>> createExample() {

        List<Row> list1 = new ArrayList<>();
        list1.add(RowFactory.create("1","hy","who",1.2));
        list1.add(RowFactory.create("2","hero","me",2.3));

        List<Row> list2 = new ArrayList<>();
        list2.add(RowFactory.create("3","all","who",3.2));
        list2.add(RowFactory.create("2","me","hero",7.3));

        StructType schema = SchemaUtils.crateStrSchema("id", "name", "content").
                add("number", DataTypes.DoubleType, true);

        return new Tuple2<>(DatasetCreateUtils.quickCreateStrDs(list1, schema),DatasetCreateUtils.quickCreateStrDs(list2, schema));
    }


}
