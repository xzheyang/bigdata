package com.hy.bigdata.modules.spark.common.sql.rmdbs;


import java.util.List;

/**
 * @USER yang.he
 * @DATE ${time}
 * @文件说明                    Mysql的sql语句提供
 **/
public class MysqlDriver {

    /**
     *  获得Mysql的合并(根据主键插入和修改)语句(通过临时表,即contactJdbcTempTableName的表名)
     *
     * @param tableName         数据库表名
     * @param columnNames       需要合并的字段名
     * @return                  sql语句
     */
    public static String createMergeSqlByTemp(String tableName,List<String> columnNames){

        StringBuffer insertMySqlStr = new StringBuffer("insert into ");
        insertMySqlStr.append(tableName).append("(");

        for (int i = 0; i < columnNames.size(); i++) {
            insertMySqlStr.append(columnNames.get(i));
            if (i != columnNames.size() - 1) {
                insertMySqlStr.append(",");
            }
        }
        insertMySqlStr.append(") values select ");
        for (int i=0;i<columnNames.size();i++){
            insertMySqlStr.append(columnNames.get(i));
            if (i!=columnNames.size()-1) {
                insertMySqlStr.append(",");
            }
        }
        insertMySqlStr = insertMySqlStr.append(" from ").append(contactTempMergeTableName(tableName)).append(" T2 ");
        insertMySqlStr =insertMySqlStr.append(" on duplicate key update ");
        for (int i=0;i<columnNames.size();i++){
            insertMySqlStr.append(columnNames.get(i));
            insertMySqlStr.append("=");
            insertMySqlStr.append("T2.").append(columnNames.get(i));
            if (i!=columnNames.size()-1) {
                insertMySqlStr.append(",");
            }
        }

        return  insertMySqlStr.toString();
    }


    /**
     *      返回临储存临时表名(仅仅供merge使用)
     *
     * @param tableName     数据库表名
     * @return              临时数据库表名
     */
    public static String contactTempMergeTableName(String tableName){
        String tempTableName = tableName+"_u_t";
        if (tempTableName.length()>30){
            throw new RuntimeException("mysql表名长度不能超过60");
        }

        return tempTableName;
    }

}
