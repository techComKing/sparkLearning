/**
 * Copyright (C), 2015-2018, XXX有限公司
 * FileName: JavaSparkRDDTest3
 * Author:   lida
 * Date:     2018/7/2 15:18
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 * lida         2018/7/2 15:18     V1.0              描述
 */
package com.spark.learn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author lida
 * @create 2018/7/2
 * @since 1.0.0
 */
public class JavaSparkRDDTest3 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("rdd_test");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<String> list = new ArrayList<>();
        list.add("how are you");
        list.add("I am ok");
        list.add("do you love me");
        JavaRDD<String> input = jsc.parallelize(list);

        JavaRDD<String> output = input.flatMap(
                new FlatMapFunction<String, String>(){
                    public Iterable<String> call(String s) throws Exception{
                        String[] arr = s.split(" ");
                        return Arrays.asList(arr);
                    }
                }
        );

        List<String> resultList = output.collect();
        for(String line : resultList){
            System.out.println(line);
        }

    }

}