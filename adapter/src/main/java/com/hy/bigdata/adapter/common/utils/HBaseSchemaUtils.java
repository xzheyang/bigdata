package com.hy.bigdata.adapter.common.utils;

import com.hy.bigdata.modules.spark.structure.utils.SchemaUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

/**
 * @USER yang.he
 * @DATE ${time}
 * @文件说明            对HBase的结构支持
 *         fixme       这种虽然去耦合,但是修改代码如何和HBase统一,还是说本身两者就应该独立,靠规范统一
 **/
public class HBaseSchemaUtils {


    /**
     *      填充数据到put
     *
     * @param schema        datSet的内容格式集合<code>dataSet.schema()</code>
     * @param p             HBase的保存类
     * @param column        datSet的内容
     * @param family        HBase保存的父族
     * @param columnName    保存的字段名
     */
    public static void fillPut(StructType schema, Put p, Object column, String family, String columnName) {

        //根据原来保存的类型,转换为指定类型的put保存
        StructField columnType = SchemaUtils.getColumnType(schema, columnName);

        if (columnType.dataType().equals(DataTypes.StringType))
            p.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnName),
                    Bytes.toBytes(String.valueOf(column)));
        else if (columnType.dataType().equals(DataTypes.BooleanType))
            p.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnName),
                    Bytes.toBytes((Boolean)column));
        else if (columnType.dataType().equals(DataTypes.IntegerType))
            p.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnName),
                    Bytes.toBytes((Integer) column));
        else if (columnType.dataType().equals(DataTypes.LongType))
            p.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnName),
                    Bytes.toBytes((Long) column));
        else if (columnType.dataType().equals(DataTypes.DoubleType))
            p.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnName),
                    Bytes.toBytes((Double) column));

            //todo 类型未完待续
        else
            p.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnName),
                    Bytes.toBytes(String.valueOf(column)));


    }

    /**
     *      将row转换为put,字段为空,不保存这个label(字段)
     *
     * @param row           需要转换的row
     * @param contactKey    拼接好的key,或者是单独一个字段的key,注意这里不能填字段名,需要值
     * @param family        需要转换的父族
     * @param columns       需要转换的字段
     * @return              row没有的字段
     */
    public static Put transformPut(Row row, String contactKey, String family, List<String> columns){


        //创建put
        byte[] keyBytes = Bytes.toBytes(contactKey);
        Put p = new Put(keyBytes);


        for (String column : columns) {

            Object addColumn = row.getAs(column);

            //todo fixme 这里有问题,先紧急处理,下层只有在同一特性时,才能验证上层是否出错
            //转换字段报空,这里需要通知上层字段不会保存这个特性
            if (addColumn==null){
                continue;
                //log.warn("需要转换保存的字段为空");
            }

            //填充数据
            fillPut(row.schema(), p, addColumn, family, column);

        }

        return p;
    }


    /**
     *  将row的指定字段拼接为key,用下划线连接
     *
     * @param row   需要转换的row
     * @param keys  需要拼接为key的字段,用下划线连接
     * @param isReverseFirst      是否反转第一个key
     * @return      没有当前字段等key
     */
    public static String contactKey(Row row, List<String> keys, boolean isReverseFirst){

        //连接key
        //todo 异常,如果没有这个字段
        StringBuffer contactKey = new StringBuffer();
        //抽取需要保存为key的字段,下划线连接
        for (int i = 0; i < keys.size(); i++) {
            //todo 默认会强转为前面的类型,需要一个类
            Object keyObject = row.getAs(keys.get(i));
            if (keyObject == null)
                throw new RuntimeException("key值缺失,或key的某个拼接字段缺失");
            String keyString = String.valueOf(keyObject);

            if (isReverseFirst&&i==0){
                keyString = new StringBuffer(keyString).reverse().toString();
            }
            contactKey.append(keyString);
            if (i != keys.size() - 1) {
                contactKey.append("_");
            }
        }

        return contactKey.toString();
    }


}
