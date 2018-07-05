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
 * 〈RDD.reduce() 接收一个函数作为参数，这个函数要操作两个RDD的元素类型的数据并返回一个同样类型的新元素。
 * 一个简单的例子就是函数+，可以用它来对我们的RDD进行累加。使用reduce(),可以很方便地计算出RDD中所有元素的总和，元素的个数，以及其他类型的聚合操作。〉
 *
 * @author lida
 * @create 2018/7/2
 * @since 1.0.0
 */
public class JavaSparkRDDTest7 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("test_map_filter");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10));

        //求RDD数据集所有元素和
        Integer sum = rdd.reduce(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1+v2;
                    }
                }
        );

        System.out.println(sum);

    }

}

