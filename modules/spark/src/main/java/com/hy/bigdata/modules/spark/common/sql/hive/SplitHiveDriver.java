package com.hy.bigdata.modules.spark.common.sql.hive;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.catalyst.TableIdentifier;
import scala.Some;
import scala.Tuple2;

/**
 * @user yang.he
 * @date 2019/8/29
 * @introduce
 **/
public class SplitHiveDriver {

    /**
     *  转换表为tuple2形式
     *
     * @param hiveTable 库.表名
     * @return
     */
    public static Tuple2<String,String> splitTable(String hiveTable) {

        String[] split = hiveTable.split("\\.");
        if (split.length!=2){
            throw new RuntimeException("表"+hiveTable+"不为[库.表名]格式");
        }

        return Tuple2.apply(split[0],split[1]);
    }


    /**
     *  转换表为tuple2形式
     *
     * @param hiveTable 库.表名
     * @return
     */
    public static TableIdentifier getTableIdentifier(String hiveTable) {

        String[] split = StringUtils.split(hiveTable,".");
        if (split.length!=2){
            throw new RuntimeException("表"+hiveTable+"不为[库.表名]格式");
        }

        return new TableIdentifier(split[1], new Some<>(split[0]));
    }

}
