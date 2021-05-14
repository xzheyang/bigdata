package com.hy.bigdata.modules.spark.component.rdbms;

import com.hy.bigdata.modules.common.environment.ConfigEnvContext;
import com.hy.bigdata.modules.spark.api.SparkSessionTemplate;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * @author yang.he
 * @date 2021-05-12 19:04
 *
 *     spark抽取rdbms相关的phase步骤操作
 *
 */
public class SparkRdbmsExtractPhase {

    final private static SparkSessionTemplate SESSION_TEMPLATE = SparkSessionTemplate.getInstance();
    final private static SparkSession SESSION = SESSION_TEMPLATE.getSession();


    /**
     *
     *  抽取rdbms关系型数据库数据
     *
     *  获得大表有两种方式:
     *      一种是按long型分片抽取,一种是sql分片抽取,这里采取分片抽取
     *
     * @param tableName 表名
     * @param sqlExpr   筛查的条件和分片方式(注意:里面不要包含重复数据)
     * @return  rdbms关系型数据库数据的dataset
     */
    public static Dataset<Row> getRdbmsTable(String tableName, String[] sqlExpr) {

        //如果未加分片条件,则用'1=1'默认单分片抽取
        if (sqlExpr==null||sqlExpr.length==0){
            sqlExpr = new String[]{ "1=1" };
        }

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", ConfigEnvContext.getStringByFilePath("sources/jdbc","user"));
        connectionProperties.put("password", ConfigEnvContext.getStringByFilePath("sources/jdbc","password"));
        connectionProperties.put("driver",ConfigEnvContext.getStringByFilePath("sources/jdbc","driver"));

        return SESSION.read().jdbc(ConfigEnvContext.getStringByFilePath("sources/jdbc","url"),tableName,sqlExpr,connectionProperties);
    }

}
