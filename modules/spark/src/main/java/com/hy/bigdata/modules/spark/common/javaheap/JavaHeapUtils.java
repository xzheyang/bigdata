package com.hy.bigdata.modules.spark.common.javaheap;


/**
 * @author  yang.he
 * @date    2020-05-12 19:04
 *
 *    关于javaHeap相关的工具类
 *
 */
public class JavaHeapUtils {


    /**
     *  控制层有几层,用来通过堆栈定位
     */
    public static Integer CONTROLLER_PLIES = 1;

    /**
     *  获得调用方法的名字
     *
     * @param isController 是否是controller层调用
     */
    public static String getInvokeName(boolean isController) {

        //获得堆栈
        StackTraceElement[] use =Thread.currentThread().getStackTrace();

        //根据是否调用,返回层数

        if (isController){
            Integer invoke = use.length-CONTROLLER_PLIES-1;
            if (invoke<=0){
                throw new RuntimeException("未找到调用方法的定位,检查参数是否正确");
            }

            return use[invoke].getClassName();
        }

        return use[use.length-1].getClassName();
    }


    /**
     *  获得调用方法上层的所有类名,方法名,调用行数
     *
     */
    public static void getInvokeLines(){
        //获得堆栈
        StackTraceElement[] stacks =Thread.currentThread().getStackTrace();
        for (StackTraceElement stack:stacks){
            System.out.println("class=["+stack.getClassName()+"],methodName=["+stack.getMethodName()+"],lineNumber=["+stack.getLineNumber()+"]");
        }

    }

}
