package com.hy.bigdata.modules.spark.common.sql.hive;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;

/**
 * @USER yang.he
 * @DATE ${time}
 * @文件说明               hive创建式语句的驱动
 **/
public class CreateSqlDriver {


    //获得hive临时视图,防止多个表同时导入出错
    public static String createTempTableName(String tableName){

        if (StringUtils.contains(tableName,".")){
            String[] split = StringUtils.split(tableName,".");
            return  "ti"+split[split.length-1];
        }
        return  "ti"+tableName;
    }

    //获得hive临时视图,用于每表复用
    public static String createTempViewName(String tableName){

        if (StringUtils.contains(tableName,".")){
            String[] split = StringUtils.split(tableName,".");
            return  "tpvi".concat(split[split.length-1]);
        }
        return  "tpvi".concat(tableName);
    }


    //获得hive用于事务的插入表名,用于临时表插入校验,全部放入trans层
    public static String createTransTableName(String tableName){

        if (StringUtils.contains(tableName,".")){
            String[] split = StringUtils.split(tableName,".");
            return  "trans.".concat(split[split.length-1]);
        }
        return  "trans.".concat(tableName);
    }



    //todo 这里可拆分一个字段语句
    public static String createTableSql(StructType schema, String table, String partitionCol, boolean isParquet){

        StringBuffer sqlStr = new StringBuffer("CREATE TABLE IF NOT EXISTS ").
                append(table).append(" (");

        Iterator<StructField> iterator = schema.iterator();
        while (iterator.hasNext()){
            StructField sf = iterator.next();

            if (StringUtils.equals(sf.name(),partitionCol))
                continue;
            sqlStr.append(" ");
            sqlStr.append(sf.name());
            sqlStr.append(" ");
            sqlStr.append(sf.dataType().sql());
            sqlStr.append(", ");
        }

        StringBuffer result = sqlStr.delete(StringUtils.lastIndexOf(sqlStr, ","), sqlStr.length()-1);
        result.append(")");

        if (isParquet)
            result.append(" STORED AS PARQUET ");

        return sqlStr.toString();
    }

    //fixme 建表给默认值 待修改 可能是现在hive版本不支持
    public static String createTableSqlWithDefaultValue(StructType schema, String table, String partitionCol, boolean isParquet, String columnname, String defaultValue){

        StringBuffer sqlStr = new StringBuffer("CREATE TABLE IF NOT EXISTS ").
                append(table).append(" (");

        Iterator<StructField> iterator = schema.iterator();
        while (iterator.hasNext()){
            StructField sf = iterator.next();

            if (StringUtils.equals(sf.name(),partitionCol))
                continue;
            sqlStr.append(" ");
            sqlStr.append(sf.name());
            sqlStr.append(" ");
            sqlStr.append(sf.dataType().sql());
            if(StringUtils.equalsIgnoreCase(sf.name(),columnname)){
                sqlStr.append(" ");
                sqlStr.append("default "+defaultValue);
            }
            sqlStr.append(", ");
        }

        StringBuffer result = sqlStr.delete(StringUtils.lastIndexOf(sqlStr, ","), sqlStr.length()-1);
        result.append(")");

        if (isParquet)
            result.append(" STORED AS PARQUET ");

        return sqlStr.toString();
    }


}
