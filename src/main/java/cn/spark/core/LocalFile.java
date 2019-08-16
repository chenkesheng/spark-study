package cn.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * @Author: cks
 * @Date: Created by 2019/8/8 10:00
 * @Package: cn.spark.core
 * @Description: 使用本地文件创建RDD ->    统计文本文件字数
 */
public class LocalFile {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LocalFile")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("E://IdeaProjects//spark-study//src//main//java//spark.txt");
        // 统计文本文件内的字数
        JavaRDD<Integer> lineLength = lines.map(new Function<String, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        });

        int count = lineLength.reduce(new Function2<Integer, Integer, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("文件总字数是:" + count);
        // 关闭JavaSparkContext
        sc.close();
    }
}
