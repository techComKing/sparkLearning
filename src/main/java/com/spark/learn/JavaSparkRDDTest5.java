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

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author lida
 * @create 2018/7/2
 * @since 1.0.0
 */
public class JavaSparkRDDTest5 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("test_map_filter");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1,2,3,4));

        //map() 转化操作map()接收一个函数，把这个函数用于RDD中的每个元素，将函数的返回结果作为结果RDD中对应的元素。关键词：转化
        JavaRDD<Integer> result = rdd.map(
                new Function<Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1) throws Exception {
                        return v1*v1;
                    }
                }
        );
        //filter() 转化操作filter()接受一个函数，并将RDD中满足该函数的元素放入新的RDD中返回。关键词：过滤
        JavaRDD<Integer> result2 = rdd.filter(
                new Function<Integer, Boolean>() {
                    @Override
                    public Boolean call(Integer v1) throws Exception {
                        return v1!=1;
                    }
                }
        );
        //flatMap() 有时候，我们希望对每个输入元素生成多个输出元素。实现该功能的操作叫做flatMap()。
        // 和map()类似，我们提供给flatMap()的函数被分别应用到了输入的RDD的每个元素上。不过返回的不是一个元素，
        // 而是一个返回值序列的迭代器。输出的RDD倒不是由迭代器组成的。我们得到的是一个包含各个迭代器可以访问的所有元素的RDD。
        // flatMap()的一个简单用途是将输入的字符串切分成单词
        JavaRDD<String> rdd2 = jsc.parallelize(Arrays.asList("hello world","hello you","world i love you"));
        JavaRDD<String> result3 = rdd2.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterable<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(" "));
                    }
                }
        );

        System.out.println(StringUtils.join(result.collect(),","));
        System.out.println(StringUtils.join(result2.collect(),","));
        System.out.println(StringUtils.join(result3.collect(),"\n"));
    }

}