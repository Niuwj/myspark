package nwj;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class WordCount {
	private static final Pattern SPACE = Pattern.compile("\t");
	public static void main(String[] args) {
		System.out.println("OK");
		//屏蔽日志
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
		String myFile = "data/access.txt";
		wordCount(myFile);
	}

	private static void wordCount(String fileName) {
		SparkConf sparkConf = new SparkConf().setAppName("wordCount").setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = sparkContext.textFile(fileName);
		JavaPairRDD<String, Integer> counts = lines
				.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator())
				.mapToPair(word -> new Tuple2<>(word, 1))
				.reduceByKey(Integer::sum);
		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<String, Integer> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}
	}
}
