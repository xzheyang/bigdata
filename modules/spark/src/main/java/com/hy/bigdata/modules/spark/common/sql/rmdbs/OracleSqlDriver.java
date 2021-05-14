package com.hy.bigdata.modules.spark.common.sql.rmdbs;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @USER yang.he
 * @DATE ${time}
 * @文件说明            oracle的sql语句提供
 **/
public class OracleSqlDriver {

    /**
     *  获得Oracle的合并(插入和修改)语句
     *
     * @param tableName         数据库表名
     * @param columnNames       需要插入的字段名
     * @param key               指定合并的key
     * @param isTableInsert     是否是临时表表插入(需要依靠<code>MergeSqlDriver.contactJdbcTempTableName(String tableName)</code>临时表)
     * @return                  sql语句
     */
    public static String createMergeSql(String tableName, List<String> columnNames, String key, boolean isTableInsert){

        StringBuffer insertOracleStr = new StringBuffer("MERGE INTO ").append(tableName).append(" T1 USING (SELECT ");

        if (!isTableInsert) {
            for (int i = 0; i < columnNames.size(); i++) {
                insertOracleStr.append("'?' AS ").append(columnNames.get(i));
                if (i != columnNames.size() - 1) {
                    insertOracleStr.append(",");
                } else {
                    insertOracleStr.append(" FROM dual) T2 ON (");
                }
            }
        } else {
            for (int i = 0; i < columnNames.size(); i++) {
                insertOracleStr.append(columnNames.get(i));
                if (i != columnNames.size() - 1) {
                    insertOracleStr.append(",");
                } else {
                    insertOracleStr.append(" FROM ").append(contactTempMergeTableName(tableName)).append(") T2 ON (");
                }
            }
        }

        insertOracleStr.append("T1.").append(key).append("=").append("T2.").append(key);

        insertOracleStr.append(" ) WHEN MATCHED THEN UPDATE SET ");
        for (int i = 0; i < columnNames.size(); i++) {
            String col = columnNames.get(i);
            if (StringUtils.equals(col, key)) {
                continue;
            }
            insertOracleStr.append("T1.").append(col).append("=T2.").append(col);
            if (i != columnNames.size() - 1) {
                insertOracleStr.append(",");
            }
        }
        insertOracleStr.append("  WHEN NOT MATCHED THEN INSERT (");
        for (int i = 0; i < columnNames.size(); i++) {
            String col = columnNames.get(i);
            insertOracleStr.append(col);
            if (i != columnNames.size() - 1) {
                insertOracleStr.append(",");
            }
        }
        insertOracleStr.append(") VALUES (");
        for (int i = 0; i < columnNames.size(); i++) {
            String col = columnNames.get(i);
            insertOracleStr.append("T2.").append(col);
            if (i != columnNames.size() - 1) {
                insertOracleStr.append(",");
            } else {
                insertOracleStr.append(")");
            }
        }

        return insertOracleStr.toString();
    }


    /**
     *  获得Oracle的合并(插入和修改)语句
     *
     * @param tableName         数据库表名
     * @param columnNames       需要插入的字段名
     * @param keys               指定合并的key(支持多个key)
     * @param isTableInsert     是否是临时表表插入(需要依靠<code>MergeSqlDriver.contactJdbcTempTableName(String tableName)</code>临时表)
     * @return                  sql语句
     */
    public static String createMergeSql(String tableName, List<String> columnNames, List<String> keys, boolean isTableInsert){

        if (keys == null||keys.size()==0) {
            throw new RuntimeException("使用插件出错,不能传入空");
        }
        StringBuffer insertOracleStr = new StringBuffer("MERGE INTO ").append(tableName).append(" T1 USING (SELECT ");

        if (!isTableInsert) {
            for (int i = 0; i < columnNames.size(); i++) {
                insertOracleStr.append("'?' AS ").append(columnNames.get(i));
                if (i != columnNames.size() - 1) {
                    insertOracleStr.append(",");
                } else {
                    insertOracleStr.append(" FROM dual) T2 ON (");
                }
            }
        } else {
            for (int i = 0; i < columnNames.size(); i++) {
                insertOracleStr.append(columnNames.get(i));
                if (i != columnNames.size() - 1) {
                    insertOracleStr.append(",");
                }
                else {
                    insertOracleStr.append(" FROM ").append(contactTempMergeTableName(tableName)).append(") T2 ON (");
                }
            }
        }
        for (int i = 0; i < keys.size(); i++) {
            insertOracleStr.append("T1.").append(keys.get(i)).append("=").append("T2.").append(keys.get(i));
            if (i != keys.size() - 1) {
                insertOracleStr.append(" AND ");
            }
        }


        insertOracleStr.append(" ) WHEN MATCHED THEN UPDATE SET ");
        for (int i = 0; i < columnNames.size(); i++) {
            String col = columnNames.get(i);
            if (keys.stream().anyMatch(s -> StringUtils.equalsIgnoreCase(s, col))) {
                continue;
            }
            insertOracleStr.append("T1.").append(col).append("=T2.").append(col);
            if (i != columnNames.size() - keys.size()) {
                insertOracleStr.append(",");
            }
        }
        insertOracleStr.append("  WHEN NOT MATCHED THEN INSERT (");
        for (int i = 0; i < columnNames.size(); i++) {
            String col = columnNames.get(i);
            insertOracleStr.append(col);
            if (i != columnNames.size() - 1) {
                insertOracleStr.append(",");
            }
        }
        insertOracleStr.append(") VALUES (");
        for (int i = 0; i < columnNames.size(); i++) {
            String col = columnNames.get(i);
            insertOracleStr.append("T2.").append(col);
            if (i != columnNames.size() - 1) {
                insertOracleStr.append(",");
            } else {
                insertOracleStr.append(")");
            }
        }

        return insertOracleStr.toString();
    }



    /**
     *      返回临储存临时表名(仅仅供merge使用)
     *
     * @param tableName     数据库表名
     * @return              临时数据库表名
     */
    public static String contactTempMergeTableName(String tableName){
        String tempTableName = tableName+"_u_t";
        if (tempTableName.length()>30) {
            throw new RuntimeException("Oracle表名长度不能超过26");
        }
        return tempTableName;
    }

    /**
     *      返回临储存临时去重表名(仅仅供merge使用)
     *
     * @param tableName     数据库表名
     * @return              临时数据库表名
     */
    public static String contactTempMergeDistinctTableName(String tableName){
        String tempTableName = tableName+"_d_t";
        if (tempTableName.length()>30) {
            throw new RuntimeException("Oracle表名长度不能超过26");
        }
        return tempTableName;
    }

}
