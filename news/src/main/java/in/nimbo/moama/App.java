package in.nimbo.moama;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("alireza").setMaster("spark://94.23.214.93:7077");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4));
        System.out.println(rdd.reduce((a, b) -> a + b));
    }
}
