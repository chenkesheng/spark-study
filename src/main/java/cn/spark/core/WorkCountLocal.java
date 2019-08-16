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
 * @Date: Created by 2019/7/31 16:20
 * @Package: cn.spark.core
 * @Description:
 */
public class WorkCountLocal {

    public static void main(String[] args) {
        //编写spark应用程序
        //创建sparkConf对象    设置spark应用的配置信息
        //使用设置master可以设置spark应用程序要连接的spark集群的Master节点的url；但是如果设置为local则代表在本地运行
        SparkConf conf = new SparkConf().setAppName("WorkCountLocal")
                .setMaster("local");

        /**
         *         创建 JavaSparkContext对象
         *         在spark中, sparkContext 是spark所有功能的入口，无论是java还是scala还是别的语言
         *         都必须要有个 sparkContext ,它的主要作用，包括初始化spark应用程序所需的一些核心组件，包括
         *         调度器（DAGSchedule、TaskScheduler),还会到spark节点去注册，等等
         *         一句话 sparkContext ，是spark应用中最重要的一个对象
         *         但是在Spark中，编写不同类型的spark应用程序 使用的 sparkContext 是不同的，如果使用scala
         *         使用的是原生的 sparkContext 对象
         *         使用java， 那就是 JavaSparkContext 对象
         *         如果是开发Spark sql 程序， 那么就是SQLContext 、 HiveContext
         *         如果是 Spark steaming 程序，那么就是它独有的 sparkContext
         *         以此类推
         */

        JavaSparkContext context = new JavaSparkContext(conf);

        /**
         *  要针对输入源（hdfs文件 本地文件 等等），创建一个初始RDD
         *  输入源中的数据会打散，分配到每个partition中，从而形成一个初始的分布式数据集
         *  sparkContext 中，用于根据文件类型的输入源创建RDD的方法，叫做 textFile() 方法
         *  在这里 RDD中 有元素的概念， 如果hdfs是本地文件呢，创建RDD 每一个元素相当于文件里的一行
         */

        JavaRDD<String> lines = context.textFile("E://IdeaProjects//spark-study//src//main//java//spark.txt");

        /**
         *   对初始RDD进行 transformation 操作，也就是一些计算操作
         *   通常操作会创建 Function 并配合RDD的map/flatMap等来执行计算
         *   function 通常比较简单，则会创建function的匿名内部类
         *   但是如果function比较复杂，则会单独创建一个类，作为实现这个function接口的类
         *
         *   先将每一行拆分成一个单词
         *   FlatMapFunction有两个参数 分表代表输入和输出类型
         *   我这 输入是String, 因为是一行行的文本，输出也是String,因为是每一行的文本
         *   我这里先介绍flatMap算子的作用，其实就是将RDD的一个元素，给拆分成一个或多个元素
         */

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {


            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
    });

        /**
         * 需要将每一个单词，映射为（单词,1)的这种格式
         * 因为只有这样后面才能根据单词作为key，来进行每个单词出现次数的累加
         * mapToPair其实就是将每个元素映射为（v1,v2)Tuple2类型的元素
         * 就是scala中的Tuple类型的Tuple2
         * mapToPair这个算子，要求的是与PairFunction配合使用，第一个参数类型代表了输入的参数类型
         * 第二个和第三个泛型参数类型，代表的输出的Tuple2的第一个类型和第二个值的类型
         * JavaPairRDD的两个泛型参数，分别代表了tuple元素的第一个值和第二个值的类型
         */

        JavaPairRDD<String, Integer> pairs = words.mapToPair(

                new PairFunction<String, String, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<>(word, 1);
                    }
                });

        /**
         * 需要以单词作为key 统计每个单词出现的次数
         * 这里要使用reduceByKey这个算子，对每个key对应的value，都进行reduce操作
         * 比如 JavaPairRDD 中有几个元素， 分别为(hello 1) (hello, 1) <world, 1)
         * reduce操作，相当于是把第一个值和第二个值进行计算，然后再将结果与第三个值进行计算
         * 比如这里的Hello，那么相当于是，首先是 1 + 1=2 ,然后在将2 + 1 = 3
         * 最后返回的 JavaPairRDD 中的元素，也是tuple，第一个值是每个Key,第二个值就是key的value reduce之后的结果相当于就是每个单词出现的次数
         */
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(

                new Function2<Integer, Integer, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                });

        /**
         * 到这里为止通过spark的算子操作，已经统计出的单词的出现次数
         * 但是之前我们使用的 flatMap mapToPair reduceByKey这种操作都叫做transformation操作
         * 一个Spark应用中，光是有transformation操作，是不行的，是不会执行的，必须要有一种叫做action
         * 接着，最后，可以使用一种叫做action操作的，比如说，foreach，来触发程序的执行
         */

        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1 + " appeared " + wordCount._2 + " times.");
            }
        });
        context.close();
    }
}
