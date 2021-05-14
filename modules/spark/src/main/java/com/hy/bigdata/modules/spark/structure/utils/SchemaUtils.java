package com.hy.bigdata.modules.spark.structure.utils;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author yang.he
 * @date 2019/10/29
 *
 *      tools of schema
 *
 *      注: 1.创建schema后可以用RowEncoder.apply(schema)生成encoder
 *          2.可以用new StructType()创建schema,然后add指定名字,类型和成为空
 *
 **/
public class SchemaUtils {


    /**
     *  转换bean类为schema结构的StructType
     *
     * @param baseSchema    需要转换需要sparkSchema的对象
     * @return  schema结构的StructType
     */
    public static StructType transform(Object baseSchema) {

        List<StructField> schemaList = new ArrayList<>();

        Field[] fields = baseSchema.getClass().getDeclaredFields();
        for (Field field : fields){
            DataType dataType = transClassToDataTypes(field.getType());
            String name = field.getName().toLowerCase();

            if (dataType==null){
                throw new RuntimeException("转换bean的对象类型为schema失败");
            }

            schemaList.add(DataTypes.createStructField(name,dataType,true));
        }


        return DataTypes.createStructType(schemaList);
    }


    /**
     *  创建string类型的schema
     *
     * @param cols 字段名
     */
    public static StructType crateStrSchema(String... cols) {
        if (cols==null||cols.length==0){
            throw new RuntimeException("创建的字符schemaCol为空");
        }

        StructType schema = new StructType();
        for(String col:cols){
            //这种写法必须要schema = schema.add(filed)的形式
            schema = schema.add(DataTypes.createStructField(col,DataTypes.StringType,true));
        }

        return schema;
    }


    /**
     *  将类转换为DataType
     *
     * @param  c 需要转换的
     * @return 返回相应属性类型的dataType
     */
    public static DataType transClassToDataTypes(Class c) {

        if (c ==  String.class){
            return DataTypes.StringType;
        }else if (c ==  Boolean.class || c==Boolean.TYPE){
            return DataTypes.BooleanType;
        }else if (c ==  Integer.class || c==Integer.TYPE){
            return DataTypes.IntegerType;
        }else if (c ==  Double.class || c==Double.TYPE){
            return DataTypes.DoubleType;
        }else if (c ==  Long.class || c==Long.TYPE){
            return DataTypes.LongType;
        }else if (c ==  Timestamp.class){
            return DataTypes.TimestampType;
        }else if (c ==  BigDecimal.class){
            return DataTypes.createDecimalType(38,6);
        }

        return null;
    }


    /**
     *  schema中是否存在colName
     *
     * @param schema    数据结构
     * @param colName   字段
     */
    public static boolean existCol(StructType schema,String colName) {
        return Arrays.asList(schema.fieldNames()).contains(colName);
    }


    /**
     *  schema中是否存在colNames
     *
     * @param schema    数据结构
     * @param colNames   字段
     */
    public static boolean existCol(StructType schema,List<String> colNames) {
        return Arrays.stream(schema.fieldNames()).anyMatch(colNames::contains);
    }


    /**
     *  根据类型获得指定的数据结构
     *
     * @param schema            数据结构
     * @param columnName       字段名
     * @return  字段数据结构
     */
    public static StructField getColumnType(StructType schema, String columnName){
        //如果没有col会报错
        int i = schema.fieldIndex(columnName);
        return schema.fields()[i];
    }


    /**
     *  获得结构所有字段名
     *
     * @param schema    元数据
     *
     */
    public static List<String> getAllColumnNames(StructType schema){
        List<String> columnNames = new ArrayList<>();

        Iterator<StructField> iterator = schema.iterator();
        while (iterator.hasNext()){
            StructField next = iterator.next();
            columnNames.add(next.name());
        }

        return columnNames;
    }


    /**
     *  检查row是否支持字段名
     *
     * @param row 列数据
     */
    public static void checkHaveSchema(Row row){
        StructType schema = row.schema();
        if (schema==null){
            throw new RuntimeException("当前DataSet(Row)不支持Schema(字段映射),需要转换");
        }
    }






}
