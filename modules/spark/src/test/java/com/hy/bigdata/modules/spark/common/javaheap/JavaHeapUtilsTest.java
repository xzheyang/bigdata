package com.hy.bigdata.modules.spark.common.javaheap;

import org.junit.Test;

/**
 * @author yang.he
 * @date 2021-05-12 16:54
 */
public class JavaHeapUtilsTest {

    @Test
    public void getInvokeName() {
        testHeapName();
    }

    @Test
    public void getInvokeParams() {
        testHeapLines("无意义","字符");
    }

    private void testHeapLines(String s, String z){
        JavaHeapUtils.getInvokeLines();
    }

    private void testHeapName(){
        String invokeName = JavaHeapUtils.getInvokeName(true);
        System.out.println(invokeName);
    }

}