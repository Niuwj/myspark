/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nwj;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class JavaWordCount {
  private static final Pattern SPACE = Pattern.compile("\t");

  public static void main(String[] args) throws Exception {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

//    if (args.length < 1) {
//      System.err.println("Usage: JavaWordCount <file>");
//      System.exit(1);
//    }
//    String fname = args[0];
    String fname = "data/access.txt";

    //spark 2.0之前的版本
//    SparkConf sparkConf = new SparkConf().setAppName("wordCount").setMaster("local");
//    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
//    JavaRDD<String> lines = sparkContext.textFile(args[0]);


    //现在SparkConf、SparkContext 和 SQLContext 都已经被封装在 SparkSession 当中，并且可以通过 builder 的方式创建
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaWordCount")
      .master("local")
      .getOrCreate();
    JavaRDD<String> lines = spark.read().textFile(fname).javaRDD();

    JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

    JavaPairRDD<String, Integer> counts = ones.reduceByKey(Integer::sum);

    List<Tuple2<String, Integer>> output = counts.collect();
    for (Tuple2<?,?> tuple : output) {
      System.out.println(tuple._1() + ": " + tuple._2());
    };
    spark.stop();
  }
}
