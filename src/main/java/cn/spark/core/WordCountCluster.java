package cn.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @Author: cks
 * @Date: Created by 2019/8/1 11:05
 * @Package: cn.spark.core
 * @Description:
 */
public class WordCountCluster {
    public static void main(String[] args) {
        /**
         *          如果要在spark集群上运行，需要修改的，只有两个地方
         *          第一，将SparkConf的setMaster()方法给删掉，默认它自己会去连接
         *          第二，我们针对的不是本地文件了，修改为hadoop hdfs上的真正的存储大数据的文件
         *
         *          实际执行步骤：
         *          1、将spark.txt文件上传到hdfs上去
         *          2、使用我们最早在pom.xml里配置的maven插件，对spark工程进行打包
         *          3、将打包后的spark工程jar包，上传到机器上执行
         *          4、编写spark-submit脚本
         *          5、执行spark-submit脚本，提交spark应用到集群执行
         */

        SparkConf conf = new SparkConf().setAppName("WordCountCluster");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("hdfs://192.168.88.160:9000/spark.txt");

        //这是lambda表达式的写法
        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>)
                line -> Arrays.asList(line.split(" ")).iterator());

        //这是lambda表达式的写法
        JavaPairRDD<String, Integer> pairs = words.mapToPair((PairFunction<String, String, Integer>)
                word -> new Tuple2<>(word, 1));

        //这是不用lamba的匿名内部类
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(

                new Function2<Integer, Integer, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }

                });

        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1 + " appeared " + wordCount._2 + " times.");
            }

        });

        sc.close();
    }
}
