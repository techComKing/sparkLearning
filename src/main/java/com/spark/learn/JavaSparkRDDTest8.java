/**
 * Copyright (C), 2015-2018, XXX有限公司
 * FileName: JavaSparkRDDTest5
 * Author:   lida
 * Date:     2018/7/2 16:03
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 * lida         2018/7/2 16:03     V1.0              描述
 */
package com.spark.learn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;

/**
 * 〈一句话功能简述〉<br> 
 * 〈fold()操作
    接收一个与reduce()接收的函数签名相同的函数，再加上一个初始值来作为每个分区第一次调用时的结果。你所提供的初始值应当是你提供的操作的单位元素，
    也就是说，使用你的函数对这个初始值进行多次计算不会改变结果（例如+对应的0，*对应的1，或者拼接操作对应的空列表）〉
 *
 * @author lida
 * @create 2018/7/2
 * @since 1.0.0
 */
public class JavaSparkRDDTest8 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("test_map_filter");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10));

        //计算RDD数据集中所有元素的和
        //zeroValue=0;//求和时，初始值为0。
        Integer sum = rdd.fold(0,
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1+v2;
                    }
                }
        );
        System.out.println("sum = "+sum);

        //zeroValue=1；//求积时，初始值为1。
        Integer result = rdd.fold(1,
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1*v2;
                    }
                }
        );
        System.out.println("result = "+result);

    }

}

