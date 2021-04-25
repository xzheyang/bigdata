package com.hy.bigdata.modules.spark.examples.common.udf.functions;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.api.java.UDF2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * @author yang.he
 * @date 2021-04-14 19:55
 *
 * 自定义函数
 * 根据splitStr对字符进行去除第一个重复字符的处理,
 * 即为重复过的字符的数据 eg: deleteFirstStrUdf('a,a,b,a') ==> a,a,b
 *
 */
public class DeleteFirstStrUdf implements UDF2<String,String,String> {
    @Override
    public String call(String inputStr, String splitStr) throws Exception {
        if (StringUtils.isBlank(inputStr)){
            return inputStr;
        }
        if (splitStr==null){
            throw new RuntimeException("deleteFirstStrUdf函数传入的splitStr为空");
        }


        List<String> resList = new ArrayList<>();
        HashSet<String> fistKey = new HashSet<>();

        //如果是第一次出现key到fistKey,否则加入resList里
        for (String key : Splitter.on(splitStr).split(inputStr)) {
            if (fistKey.contains(key)) {
                resList.add(key);
            }else {
                fistKey.add(key);
            }
        }

        return Joiner.on(splitStr).join(resList);
    }
}
