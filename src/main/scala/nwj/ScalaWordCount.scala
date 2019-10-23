//package com.nwj.test
//
//import org.apache.spark.{SparkConf, SparkContext}
////
////object WordCount {
////  def main(args: Array[String]): Unit = {
////    val wordFile = "D:/people.txt"
////    val conf = new SparkConf().setMaster("local").setAppName("wordcount");
////    val sc = new SparkContext(conf)
////    val input = sc.textFile(wordFile, 2).cache()
////    val lines = input.flatMap(line=>line.split("\\s+"))
////    val count = lines.map(word => (word,1)).reduceByKey{case (x,y)=>x+y}
////    println(count.take(5).toString)
////    for (data <- count) {
////      println(data)
////    }
////    sc.stop()
//////    val output = count.saveAsTextFile("D:/hellospark")
////  }
////}
//
//
//object WordCount {
//  def main(args: Array[String]) {
//
//    /**
//      * SparkContext 的初始化需要一个SparkConf对象
//      * SparkConf包含了Spark集群的配置的各种参数
//      */
//    val conf=new SparkConf()
//      .setMaster("local")//启动本地化计算
//      .setAppName("WordCount")//设置本程序名称
//
//    //Spark程序的编写都是从SparkContext开始的
//    val sc=new SparkContext(conf)
//    //以上的语句等价与val sc=new SparkContext("local","testRdd")
//    val data=sc.textFile("D:/people.txt")//读取本地文件
//    var result = data.flatMap(_.split(" "))//下划线是占位符，flatMap是对行操作的方法，对读入的数据进行分割
//      .map((_,1))//将每一项转换为key-value，数据是key，value是1
//      .reduceByKey(_+_)//将具有相同key的项相加合并成一个
//
//    result.collect()//将分布式的RDD返回一个单机的scala array，在这个数组上运用scala的函数操作，并返回结果到驱动程序
//      .foreach(println)//循环打印
//
//    Thread.sleep(10000)
//    result.saveAsTextFile("D:/wordcountres")
//    println("OK,over!")
//  }
//}

package nwj

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCount {
  def main(args: Array[String]): Unit = {
    println("ok")
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //设置本机Spark配置
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    //创建Spark上下
    val sc = new SparkContext(conf)
    //从文件中获取数据
    val input = sc.textFile("D:\\people.txt")
    println("--------------start")
    //分析并排序输出统计结果
    input.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((x, y) => x + y).sortBy(_._2,ascending = false).foreach(println)
    println("--------------end")
  }
}


//object ScalaWordCount {
//  def main(args: Array[String]) {
//
//    /**
//      * SparkContext 的初始化需要一个SparkConf对象
//      * SparkConf包含了Spark集群的配置的各种参数
//      */
//    val conf=new SparkConf()
//      .setMaster("local")//启动本地化计算
//      .setAppName("WordCount")//设置本程序名称
//
//    //Spark程序的编写都是从SparkContext开始的
//    val sc=new SparkContext(conf)
//    //以上的语句等价与val sc=new SparkContext("local","testRdd")
//    val data=sc.textFile("D:/people.txt")//读取本地文件
//    var result = data.flatMap(_.split(" "))//下划线是占位符，flatMap是对行操作的方法，对读入的数据进行分割
//      .map((_,1))//将每一项转换为key-value，数据是key，value是1
//      .reduceByKey(_+_)//将具有相同key的项相加合并成一个
//
//    result.collect()//将分布式的RDD返回一个单机的scala array，在这个数组上运用scala的函数操作，并返回结果到驱动程序
//      .foreach(println)//循环打印
//
//    Thread.sleep(10000)
//    result.saveAsTextFile("D:/wordcountres")
//    println("OK,over!")
//  }
//}

//import org.apache.spark._
//import org.apache.spark.SparkContext._
//
//object ScalaWordCount {
//  def main(args: Array[String]) {
//    val inputFile = args(0)
//    val outputFile = args(1)
//    val conf = new SparkConf().setAppName("wordCount")
//    // Create a Scala Spark Context.
//    val sc = new SparkContext(conf)
//    // Load our input data.
//    val input =  sc.textFile(inputFile)
//    // Split up into words.
//    val words = input.flatMap(line => line.split(" "))
//    // Transform into word and count.
//    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
//    // Save the word count back out to a text file, causing evaluation.
//    counts.saveAsTextFile(outputFile)
//  }
//}

//package nwj
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf
//import org.apache.log4j.{Level,Logger}
//
//object WordCount {
//  def main(args: Array[String]) {
//    //屏蔽日志
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
//    val inputFile =  "D:/people.txt"
//    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
//    val sc = new SparkContext(conf)
//    val textFile = sc.textFile(inputFile)
//    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
//    wordCount.foreach(println)
//  }
//}


