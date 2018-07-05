/**
 * Copyright (C), 2015-2018, XXX有限公司
 * FileName: JavaSparkRDDTest10
 * Author:   lida
 * Date:     2018/7/3 10:44
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 * lida         2018/7/3 10:44     V1.0              描述
 */
package com.spark.learn;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;

import java.util.Arrays;

/**
 * 〈一句话功能简述〉<br> 
 * 求RDD每个对象的平方值
 * @author lida
 * @create 2018/7/3
 * @since 1.0.0
 */
public class JavaSparkRDDTest11 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("persistenceRDD");

        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd =  javaSparkContext.parallelize(Arrays.asList(1,2,3,4,5));

        JavaDoubleRDD javaDoubleRDD = rdd.mapToDouble(
                new DoubleFunction<Integer>() {
                    @Override
                    public double call(Integer integer) throws Exception {
                        return integer*integer;
                    }
                }
        );
        System.out.println(StringUtils.join(javaDoubleRDD.collect(),"|"));
        System.out.println("最大值max = "+javaDoubleRDD.max());
        System.out.println("第一个first = "+javaDoubleRDD.first());
        System.out.println("平均值mean = "+javaDoubleRDD.mean());
        System.out.println("最小值min = "+javaDoubleRDD.min());
        System.out.println("(stats 统计学方法，同时计算RDD中的平均值，方差，标准差，最大值和最小值)stats = "+javaDoubleRDD.stats());
        System.out.println("(调用stats 函数，取得RDD中的标准差，divides by N-1)sampleStdev = "+javaDoubleRDD.sampleStdev());
        System.out.println("sampleVariance = "+javaDoubleRDD.sampleVariance());
        System.out.println("(调用stats 函数，取得RDD中的标准差，divides by N)stdev = "+javaDoubleRDD.stdev());
        System.out.println("累加和sum = "+javaDoubleRDD.sum());
        System.out.println("数量count = "+javaDoubleRDD.count());
        System.out.println("top(2) = "+javaDoubleRDD.top(2));
    }

}