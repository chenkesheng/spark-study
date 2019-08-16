package cn.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: cks
 * @Date: Created by 2019/8/12 10:46
 * @Package: cn.spark.core
 * @Description: 广播变量
 */
public class BroadcastVariable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("BroadcastVariable").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 在java中，创建共享变量，就是调用SparkContext的broadcast()方法
        // 获取的返回结果是Broadcast<T>类型
        final int factor = 3;
        final Broadcast<Integer> factorBroadcast = sc.broadcast(factor);

        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6);

        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        // 让集合中的每个数字，都乘以外部定义的那个factor
        JavaRDD<Integer> multipleNumbers = numbers.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v) throws Exception {
                // 使用共享变量时，调用其value()方法，即可获取其内部封装的值
                int factor = factorBroadcast.value();
                return v * factor;
            }
        });

        multipleNumbers.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer t) throws Exception {
                System.out.println(t);
            }
        });

        sc.close();
    }
}
