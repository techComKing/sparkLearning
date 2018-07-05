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

import java.io.Serializable;
import java.util.Arrays;

/**
 * 〈一句话功能简述〉<br> 
 * 〈aggregate()函数返回值类型不必与所操作的RDD类型相同。
 与fold()类似，使用aggregate()时，需要提供我们期待返回的类型的初始值。然后通过一个函数把RDD中的元素合并起来放入累加器。
 考虑到每个节点是在本地进行累加的，最终，还需要提供第二个函数来将累加器两两合并。〉
 *
 * 实现求出RDD对象集的平均数
 * @author lida
 * @create 2018/7/2
 * @since 1.0.0
 */
public class JavaSparkRDDTest9 implements Serializable{
    public int total;
    public int num;
    public JavaSparkRDDTest9(int total , int num){
        this.total = total;
        this.num = num;
    }

    public double avg(){
        return total/num;
    }

    static Function2<JavaSparkRDDTest9, Integer, JavaSparkRDDTest9> addAndCount =
            new Function2<JavaSparkRDDTest9, Integer, JavaSparkRDDTest9>() {
                @Override
                public JavaSparkRDDTest9 call(JavaSparkRDDTest9 a, Integer x) throws Exception {
                    a.total += x;
                    a.num ++;
                    return a;
                }
            };

    static Function2<JavaSparkRDDTest9, JavaSparkRDDTest9, JavaSparkRDDTest9> combine =
            new Function2<JavaSparkRDDTest9, JavaSparkRDDTest9, JavaSparkRDDTest9>() {
                @Override
                public JavaSparkRDDTest9 call(JavaSparkRDDTest9 a, JavaSparkRDDTest9 b) throws Exception {
                    a.total += b.total;
                    a.num += b.num;
                    return a;
                }
            };

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaSparkRDDTest9 intial = new JavaSparkRDDTest9(0,0);
        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));
        JavaSparkRDDTest9 result = rdd.aggregate(intial, addAndCount, combine);
        System.out.println(result.avg());
    }

}

