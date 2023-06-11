package com.imooc.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 动态调整Join策略
 * Created by xuwei
 */
object AQEjJoinScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")

    //获取SparkSession，为了操作SparkSQL
    val sparkSession = SparkSession
      .builder()
      .appName("AQEjJoinScala")
      .config(conf)
      //禁用AQE机制
      //.config("spark.sql.adaptive.enabled","false")
      //禁用自动广播机制
      .config("spark.sql.adaptive.autoBroadcastJoinThreshold","-1")
      .getOrCreate()

    //读取Json格式数据，获取DataFrame
    val jsonDf1 = sparkSession.read.json("D:\\spark_json_t1.dat")
    val jsonDf2 = sparkSession.read.json("D:\\spark_json_t2.dat")
    //创建临时表
    jsonDf1.createOrReplaceTempView("t1")
    jsonDf2.createOrReplaceTempView("t2")
    //执行SQL语句
    val sql =
      """
        |select
        |	t1.id,
        |	t2.name,
        |	t1.score
        |from t1 join t2
        |	  on t1.id = t2.id
        |	  and t2.city like 'bj'
        |""".stripMargin
    sparkSession.sql(sql).write.format("json").save("hdfs://bigdata01:9000/aqe/join_"+System.currentTimeMillis())

    //让程序一直运行，这样本地的Spark任务界面就可以一直查看
    while (true){
      ;
    }
  }

}
