/**
 * Copyright (C), 2015-2018, XXX有限公司
 * FileName: JavaWordCountSparkCore
 * Author:   lida
 * Date:     2018/6/28 16:39
 * Description: JavaWordCountSparkCore
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 * lida         2018/6/28 16:39     V1.0              描述
 */
package com.spark.learn;

/**
 * 〈一句话功能简述〉<br> 
 * 〈JavaWordCountSparkCore〉
 *
 * @author lida
 * @create 2018/6/28
 * @since 1.0.0
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
   * Java实现Spark的WordCount程序
   * Created by ibf on 02/15.
  */
public class JavaWordCountSparkCore {

    public static void main(String[] args) {
        String resultHDFSSavePath = "/beifeng/spark/result/wordcount/" + System.currentTimeMillis();
        // 1. 创建SparkConf配置信息
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("spark-wordcount");

        // 2. 创建SparkContext对象，在java编程中，该对象叫做JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 从hdfs读取文件形成RDD
        // TODO: 文件路径自行给定
//        JavaRDD<String> rdd = sc.textFile("/hive/common.db/dept");
        JavaRDD<String> rdd = sc.textFile("E:\\72\\20151021\\log0 - 副本.log");

        // 4. RDD数据处理
        // TODO: 过滤特殊字符
        // 4.1 行数据的分割，调用flatMap函数
        JavaRDD<String> wordRDD = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                String line = s;
                if (line == null)
                    line = "";
                String[] arr = line.split(" ");
                return Arrays.asList(arr);
            }
        });

        // 4.2 将数据转换为key/value键值对
        /**
         * RDD的reduceByKey函数不是RDD类中，通过隐式转换后，存在于其他类中<br/>
         * Java由于不存在隐式转换，所以不能直接调用map函数进行key/value键值对转换操作，必须调用特定的函数
         * */
        JavaPairRDD<String, Integer> wordCountRDD = wordRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        // 4.3 聚合结果
        JavaPairRDD<String, Integer> resultRDD = wordCountRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 5. 结果输出
        // 5.1 结果输出到HDFS
        resultRDD.saveAsTextFile(resultHDFSSavePath);

        List<Tuple2<String, Integer>> list = resultRDD.collect();
        System.out.println("-------------------------------------------1");
        for (Tuple2<String, Integer> e : list) {
            System.out.println(e);
        }
        System.out.println("-------------------------------------------2");
        // 5.2 结果输出到MySQL
        /**
         * SparkCore RDD数据的读入是通过InputFormat来读入数据形成RDD的
         *  sc.newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
         conf: Configuration,
         fClass: Class[F],
         kClass: Class[K],
         vClass: Class[V])
         * RDD的saveASxxxx相关方法是利用OutputFormat来进行数据输出的
         * resultRDD.saveAsNewAPIHadoopDataset(conf: Configuration);
         */
//        resultRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
//
//            @Override
//            public void call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {
//                Class.forName("com.mysql.jdbc.Driver");
//                String url = "jdbc:mysql://hadoop-senior01:3306/test";
//                String username = "root";
//                String password = "123456";
//                Connection conn = null;
//                try {
//                    // 1. 创建connection连接
//                    conn = DriverManager.getConnection(url, username, password);
//
//                    // 2. 构建statement
//                    String sql = "insert into wordcount values(?,?)";
//                    PreparedStatement pstmt = conn.prepareStatement(sql);
//
//                    // 3. 结果数据输出
//                    while (tuple2Iterator.hasNext()) {
//                        Tuple2<String, Integer> t2 = tuple2Iterator.next();
//                        pstmt.setString(1, t2._1());
//                        pstmt.setLong(2, t2._2());
//
//                        pstmt.executeUpdate();
//                    }
//                } finally {
//                    // 4. 关闭连接
//                    conn.close();
//                }
//
//            }
//        });


    }
}