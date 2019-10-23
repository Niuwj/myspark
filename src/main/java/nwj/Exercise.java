package nwj;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import py4j.StringUtil;

import java.util.Arrays;
import java.util.List;

public class Exercise {
    public static void main(String[] args) {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        SparkConf sparkConf = new SparkConf().setAppName("wordCount").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<Integer> rdd = sparkContext.parallelize(Arrays.asList(1,2,3,4,5));
        JavaRDD<Integer> counts = rdd.map(s -> s*s);
        List<Integer> result = counts.collect();
        System.out.println(StringUtils.join(result, ","));
    }
}
