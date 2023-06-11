package com.imooc.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 动态优化倾斜的Join
 * Created by xuwei
 */
object AQESkewJoinScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")

    //获取SparkSession，为了操作SparkSQL
    val sparkSession = SparkSession
      .builder()
      .appName("AQESkewJoinScala")
      .config(conf)
      //禁用AQE机制
      //.config("spark.sql.adaptive.enabled","false")
      //禁用动态优化倾斜的Join（其实只要禁用了AQE机制，这个参数就不用设置了）
      //.config("spark.sql.adaptive.skewJoin.enabled","false")
      //注意：在验证动态优化倾斜的Join这个功能的时候，最好先把自适应调整Shuffle分区数量这个功能禁用，避免影响结果
      //.config("spark.sql.adaptive.coalescePartitions.enabled","false")
      //某个分区 > skewedPartitionFactor * 中位数
      .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor","5")
      //某个分区 > 100M
      .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes","100mb")
      //如果这里指定的分区大小超过了任务中倾斜的分区大小，这样就无法触发动态优化倾斜的Join这个功能了
      //建议这里设置的分区大小最大也不能超过skewedPartitionThresholdInBytes的值（100M）
      .config("spark.sql.adaptive.advisoryPartitionSizeInBytes","64mb")
      .getOrCreate()


    //读取Json格式数据，获取DataFrame
    val jsonDf1 = sparkSession.read.json("D:\\spark_json_skew_t1.dat")
    val jsonDf2 = sparkSession.read.json("D:\\spark_json_skew_t2.dat")
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
        |	on t1.id = t2.id
        |""".stripMargin
    sparkSession.sql(sql).write.format("json").save("hdfs://bigdata01:9000/aqe/skew_join_"+System.currentTimeMillis())

    //让程序一直运行，这样本地的Spark任务界面就可以一直查看
    while (true){
      ;
    }
  }
}
