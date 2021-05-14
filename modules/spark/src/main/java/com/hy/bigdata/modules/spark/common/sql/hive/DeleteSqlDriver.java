package com.hy.bigdata.modules.spark.common.sql.hive;

/**
 * @USER yang.he
 * @DATE ${time}
 * @文件说明            hive删除式语句的驱动
 **/
public class DeleteSqlDriver {

    /**
     *
     *  获得删除hive表sql语句
     *
     * @param table     库.hive表名格式
     * @return
     */
    public static String createDropSql(String table){
        return "drop table if exists "+table;
    }

    public static String createTruncateSql(String table){
        return "truncate table "+table;
    }

}
