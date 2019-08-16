package cn.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * @Author: cks
 * @Date: Created by 2019/8/12 16:12
 * @Package: cn.spark.core
 * @Description: 取最大的前3个数字
 */
public class Top3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Top3").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("E:\\IdeaProjects\\spark-study\\src\\main\\java\\top.txt");
        JavaPairRDD<Integer,String> numbers = lines.mapToPair(new PairFunction<String, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(String s) throws Exception {
                return new Tuple2<>(Integer.parseInt(s),s);
            }
        });
        JavaPairRDD<Integer, String> sortedPairs = numbers.sortByKey(false);

        JavaRDD<Integer> sorted = sortedPairs.map(new Function<Tuple2<Integer, String>, Integer>() {
            @Override
            public Integer call(Tuple2<Integer, String> t) throws Exception {
                return t._1;
            }
        });

        List<Integer> sortedNumberList = sorted.take(3);
        sortedNumberList.forEach(System.out::println);

        sc.close();
    }
}
