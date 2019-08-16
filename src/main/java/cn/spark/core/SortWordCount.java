package cn.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Int;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @Author: cks
 * @Date: Created by 2019/8/12 11:06
 * @Package: cn.spark.core
 * @Description: 排序的wordcount程序
 */
public class SortWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SortWordCount").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("E://IdeaProjects//spark-study//src//main//java//spark.txt");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        // 到这里为止，就得到了每个单词出现的次数
        // 但是，问题是，我们的新需求，是要按照每个单词出现次数的顺序，降序排序
        // wordCounts RDD内的元素是什么？应该是这种格式的吧：(hello, 3) (you, 2)
        // 我们需要将RDD转换成(3, hello) (2, you)的这种格式，才能根据单词出现次数进行排序把！

        // 进行key-value的反转映射
        JavaPairRDD<Integer, String> countWords = wordCounts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<>(t._2, t._1);
            }
        });

        // 按照key进行排序->降序排序
        JavaPairRDD<Integer, String> sortedCountWords = countWords.sortByKey(false);

        // 再次将value-key进行反转映射
        JavaPairRDD<String, Integer> sortedWordCounts = sortedCountWords.mapToPair(

                new PairFunction<Tuple2<Integer, String>, String, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                        return new Tuple2<>(t._2, t._1);
                    }

                });

        // 到此为止，我们获得了按照单词出现次数排序后的单词计数
        // 打印出来
        sortedWordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + " appears " + t._2 + " times.");
            }

        });
        sc.close();
    }
}
