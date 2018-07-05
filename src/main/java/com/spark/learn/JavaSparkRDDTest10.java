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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;

/**
 * 〈一句话功能简述〉<br> 
 * 〈持久化缓存〉
 *因为Spark RDD是惰性求值的，而有时我们希望能多次使用同一个RDD。如果简单地对RDD调用行动操作，Spark每次都会重算RDD以及它的所有依赖。这在迭代算法中消耗格外大，因为迭代算法常常会多次使用同一组数据。
 为了避免多次计算同一个RDD，可以让Spark对数据进行持久化。当我们让Spark持久化存储一个RDD时，计算出RDD的节点会分别保存它们所求出的分区数据。
 出于不同的目的，我们可以为RDD选择不同的持久化级别。默认情况下persist()会把数据以序列化的形式缓存在JVM的堆空间中

 级别 			    使用的空间	  cpu时间	是否在内存	是否在磁盘		备注
 MEMORY_ONLY			高 			低			是			否		直接储存在内存
 MEMORY_ONLY_SER 	    低			高			是			否		序列化后储存在内存里
 MEMORY_AND_DISK		低 			中等		    部分		    部分	    如果数据在内存中放不下，溢写在磁盘上
 MEMORY_AND_DISK_SER	低 			高			部分		    部分	    数据在内存中放不下，溢写在磁盘中。内存中存放序列化的数据。
 DISK_ONLY			    低			高			否			是		直接储存在硬盘里面
 * @author lida
 * @create 2018/7/3
 * @since 1.0.0
 */
public class JavaSparkRDDTest10 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("persistenceRDD");

        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd =  javaSparkContext.parallelize(Arrays.asList(1,42,23,4,5,15));

        rdd.persist(StorageLevel.MEMORY_ONLY());

        System.out.println(rdd.count());
        System.out.println(StringUtils.join(rdd.collect(),"-"));
    }

}