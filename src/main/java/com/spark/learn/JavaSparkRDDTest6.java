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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 〈一句话功能简述〉<br> 
 * 〈RDD1.cartesian(RDD2) 返回两个RDD数据集的笛卡尔集〉
 *
 * @author lida
 * @create 2018/7/2
 * @since 1.0.0
 */
public class JavaSparkRDDTest6 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("test_map_filter");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd1 = jsc.parallelize(Arrays.asList(1,2));
        JavaRDD<Integer> rdd2 = jsc.parallelize(Arrays.asList(1,2));

        JavaPairRDD<Integer, Integer> rdd = rdd1.cartesian(rdd2);

        for(Tuple2<Integer, Integer> tuple2 : rdd.collect()){
            System.out.println(tuple2._1 + "->" +tuple2._2);
        }
    }

}