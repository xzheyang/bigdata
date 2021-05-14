package com.hy.bigdata.modules.spark.structure.utils;

import com.hy.bigdata.modules.spark.api.SparkSessionTemplate;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 *
 *  快速创建dataset
 *
 * @author yang.he
 * @date 2021-04-19 16:36
 */
public class DatasetCreateUtils {

    final private static SparkSessionTemplate SESSION_TEMPLATE = SparkSessionTemplate.getInstance();
    final private static SparkSession SESSION = SESSION_TEMPLATE.getSession();
    final private static JavaSparkContext CONTEXT = SESSION_TEMPLATE.getContext();

    /**
     * 快速创建字符schema制造的Dataset
     *
     * @param lines     行数据
     * @param colNames  每个行对应的字符名
     * @return  快速创建字符schema制造的Dataset
     */
    public static Dataset<Row> quickCreateStrDs(List<Row> lines, List<String> colNames) {
        if (CollectionUtils.isEmpty(lines)||CollectionUtils.isEmpty(colNames)){
            throw new RuntimeException("创建的行数据或者对应的列名为空");
        }

        List<StructField> struct = new ArrayList<>();
        for (String colName:colNames){
            struct.add(DataTypes.createStructField(colName, DataTypes.StringType, true));
        }
        StructType schema = DataTypes.createStructType(struct);

        return SESSION.createDataFrame(lines, schema);
    }


    /**
     * 快速创建字符schema制造的Dataset
     *
     * @param lines     行数据
     * @param encoder   算子运行的schema
     * @return  快速创建encoder的schema制造的Dataset
     */
    public static Dataset<Row> quickCreateStrDs(List<Row> lines, ExpressionEncoder<Row> encoder) {
        return quickCreateStrDs(lines, encoder.schema());
    }


    /**
     * 快速创建字符schema制造的Dataset
     *
     * @param lines     行数据
     * @param schema   算子运行的schema
     * @return  快速创建encoder的schema制造的Dataset
     */
    public static Dataset<Row> quickCreateStrDs(List<Row> lines, StructType schema) {
        if (CollectionUtils.isEmpty(lines)||schema==null){
            throw new RuntimeException("创建的行数据或者对应的列名为空");
        }

        return SESSION.createDataFrame(lines, schema);
    }


}
