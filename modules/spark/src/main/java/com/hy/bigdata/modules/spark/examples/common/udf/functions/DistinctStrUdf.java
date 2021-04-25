package com.hy.bigdata.modules.spark.examples.common.udf.functions;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.api.java.UDF2;

/**
 *  @author yang.he
 *  @date 2021-04-14 21:51
 *
 *  udf函数样例
 *  对字符用splitStr分割符号去重,返回字符
 *
 */
public class DistinctStrUdf implements UDF2<String,String,String> {

    @Override
    public String call(String inputStr, String splitStr) throws Exception {
        if (StringUtils.isBlank(inputStr)){
            return inputStr;
        }
        if (splitStr==null){
            throw new RuntimeException("distinctStrUdf函数传入的splitStr为空");
        }


        return Joiner.on(splitStr).join(Sets.newLinkedHashSet(Splitter.on(splitStr).split(inputStr)));
    }

}
