/**
 * Copyright (C), 2015-2018, XXX有限公司
 * FileName: JavaSparkRDDTest
 * Author:   lida
 * Date:     2018/7/2 14:15
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 * lida         2018/7/2 14:15     V1.0              描述
 */
package com.spark.learn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.List;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author lida
 * @create 2018/7/2
 * @since 1.0.0
 */
public class JavaSparkRDDTest1 {

    public static void main(String[] args) {
        //1.创建SparkConf配置信息
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("spark_rdd_learning");
        //2.创建SparkContext对象，在java编程中，该对象叫做JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);
        //3.将列表转换为RDD对象
        List<String> list = new ArrayList<String>();
        list.add("error:a");
        list.add("error:b");
        list.add("error:c");
        list.add("warning:d");
        list.add("happy ending");
        JavaRDD<String> rdd = sc.parallelize(list);
        //4.将RDD对象rdd中有error的表项过滤出来，放在RDD对象errorLines中
        JavaRDD<String> errorLines = rdd.filter(
                new Function<String,Boolean>(){
                    public Boolean call(String v1) throws Exception{
                        return v1.contains("error");
                    }
                }
        );
        //5.遍历过滤出来的表项
        List<String> errorList = errorLines.collect();
        for(String line : errorList){
            System.out.println(line);
        }
    }

}