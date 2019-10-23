package nwj;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import py4j.StringUtil;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Exercise {
    public static void main(String[] args) {
        long t0 = System.currentTimeMillis();
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        SparkConf sparkConf = new SparkConf().setAppName("wordCount").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<Integer> rdd = sparkContext.parallelize(Arrays.asList(1,2,3,4,6,6,1,5));
        JavaDoubleRDD counts = rdd.mapToDouble(s -> (double)s * s);
        long t1 = System.currentTimeMillis();
        List<Double> result = counts.collect();
        System.out.println("t1 - t0 = " + (t1 - t0));
        System.out.println(StringUtils.join(result, ","));
//        counts.persist(StorageLevel.DISK_ONLY());
        counts.persist(StorageLevel.MEMORY_ONLY());
        long t2 = System.currentTimeMillis();
        System.out.println("t2 - t1 = " + (t2 - t1));

        System.out.println(counts.count());
        System.out.println(counts.countByValue());
        System.out.println(StringUtils.join(counts.distinct().collect(), ", "));
        System.out.println(StringUtils.join(counts.distinct().top(2), ", "));
        System.out.println(counts.max());
        System.out.println(counts.mean());
        System.out.println(counts.sum());

        System.out.println("t3 - t2 = " + (System.currentTimeMillis() - t2));
        counts.unpersist();

        // 读取文件创建 pair RDD
        JavaRDD<String> lines = sparkContext.textFile("data/access.txt");
        JavaPairRDD<String, String> pairs = lines.mapToPair(x -> new Tuple2<>(x.split("\t")[0].toUpperCase(), x));

        List<Tuple2<String, String>> output = pairs.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        };

        // 从内存数据集创建 pair RDD
        List<Tuple2<String, Integer>> data = new ArrayList<>();
        data.add(new Tuple2<>("A", 10));
        data.add(new Tuple2<>("A", 20));
        data.add(new Tuple2<>("B", 2));
        data.add(new Tuple2<>("B", 3));
        data.add(new Tuple2<>("C", 5));
        JavaPairRDD<String, Integer> pairs2 = sparkContext.parallelizePairs(data);
        JavaPairRDD<String, Tuple2<Integer, Integer>> mapValues = pairs2.mapValues(v -> new Tuple2<>(v, 1));
        JavaPairRDD<String, Tuple2<Integer, Integer>> reduceByKey = mapValues.reduceByKey((x , y ) -> new Tuple2<>(x._1+y._1, x._2+y._2));
        for (Tuple2<?,?> tuple : reduceByKey.collect()) {
            System.out.println(tuple._1() + ": " + tuple._2());
        };
    }
}
