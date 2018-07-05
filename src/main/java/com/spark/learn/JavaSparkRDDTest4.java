/**
 * Copyright (C), 2015-2018, XXX有限公司
 * FileName: JavaSparkRDDTest4
 * Author:   lida
 * Date:     2018/7/2 15:33
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 * lida         2018/7/2 15:33     V1.0              描述
 */
package com.spark.learn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 〈一句话功能简述〉<br> 
 * 〈wordcount〉
 *
 * @author lida
 * @create 2018/7/2
 * @since 1.0.0
 */
public class JavaSparkRDDTest4 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wordcount");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<String> list = new ArrayList<String>();
        list.add("how are you");
        list.add("I am ok");
        list.add("do you love me");
        JavaRDD<String> rdd = jsc.parallelize(list);

        JavaRDD<String> words = rdd.flatMap(
                new FlatMapFunction<String,String>(){
                    public Iterable<String> call(String s) throws Exception{
                        return Arrays.asList(s.split(""));
                    }
                }
        );

        JavaPairRDD<String, Integer> counts = words.mapToPair(
                new PairFunction<String,String,Integer>(){
                    public Tuple2<String, Integer> call(String s) throws Exception{
                        return new Tuple2(s, 1);
                    }
                }
        );

        JavaPairRDD<String, Integer> results = counts.reduceByKey(
                new Function2<Integer, Integer, Integer>(){
                    public Integer call(Integer v1, Integer v2) throws Exception{
                        return v1 + v2;
                    }
                }
        );

        List<Tuple2<String, Integer>> resultList = results.collect();
        for(Tuple2<String, Integer> e: resultList){
            System.out.println(e);
        }
    }

}