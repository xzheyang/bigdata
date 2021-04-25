package com.hy.bigdata.modules.common.environment;


import java.util.ResourceBundle;

/**
 *
 *  @author yang.he
 *  @date 2021-04-14 20:53
 *
 *  配置环境读取
 *
 *  todo 集群中心读取
 *  todo 后续如果ioc完善后,考虑用注解方式使用
 *
 */
public class ConfigEnvContext {


    /**
     *  读取字符配置通过文件路径
     *
     * @param filePath    文件路径  attention:现在只能读取properties文件,后面字符不需要加.properties, eg： template/spark
     * @param paramName    参数名字
     * @return    字符参数结果
     */
    public static String getStringByFilePath(String filePath,String paramName){
        return ResourceBundle.getBundle(filePath).getString(paramName);
    }


}
