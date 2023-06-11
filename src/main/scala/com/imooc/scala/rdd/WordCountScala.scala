package com.imooc.scala.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 基于Spark3.x版本的WordCount案例
 * Created by xuwei
 */
object WordCountScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCountScala")
      //.setMaster("local")

    val sc = new SparkContext(conf)

    var path = "D:\\hello.txt"
    if(args.length==1){
      path = args(0)
    }
    val linesRDD = sc.textFile(path)
    val wordsRDD = linesRDD.flatMap(_.split(" "))
    val pairRDD = wordsRDD.map((_, 1))
    val wordCountRDD = pairRDD.reduceByKey(_ + _)
    wordCountRDD.foreach(wordCount=>println(wordCount._1+"--"+wordCount._2))
    sc.stop()

  }

}
