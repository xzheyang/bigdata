package com.hy.bigdata.modules.spark.examples.common.udf.functions;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.api.java.UDF3;
import scala.collection.immutable.Map;

import java.util.LinkedHashSet;
import java.util.Set;


/**
 * @author yang.he
 * @date 2021-04-14 19:05
 *
 *  根据splitKey分割search的字符,然后寻找对应dictMap的值,结果返回去重去空的枚举值
 *       eg:   strContainMapUdf('a_b_c','_',Map(a:'1',b:'1',d:'3')) ==> '1'
 *
 */
public class StrContainMapUdf implements UDF3<String, String, Map<String, String>, String> {

    @Override
    public String call(String search,String splitKey, Map<String, String> dictMap) throws Exception {
        if (StringUtils.isBlank(search)){
            return search;
        }
        if (StringUtils.isBlank(splitKey)){
            throw new RuntimeException("strContainMapUdf函数的splitKey传入值为空");
        }

        Set<String> dictSet = new LinkedHashSet<>();
        for (String s : Splitter.on(splitKey).split(search)) {
            dictSet.add(dictMap.getOrElse(s,()->null));
        }
        dictSet.remove(null);

        return Joiner.on(splitKey).join(dictSet);
    }

}
