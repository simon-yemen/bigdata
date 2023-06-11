package com.imooc.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 自适应调整Shuffle分区数量
 * Created by xuwei
 */
object AQECoalescePartitionsScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setMaster("local")

    //获取SparkSession，为了操作SparkSQL
    val sparkSession = SparkSession
      .builder()
      .appName("AQECoalescePartitionsScala")
      .config(conf)
      //手工指定Shuffle分区数量，默认是200
      //.config("spark.sql.shuffle.partitions","10")
      //禁用AQE机制
      //.config("spark.sql.adaptive.enabled","false")
      //禁用自适应调整Shuffle分区数量（其实只要禁用了AQE机制，这个参数就不用设置了）
      //.config("spark.sql.adaptive.coalescePartitions.enabled","false")
      //设置建议的分区大小
      .config("spark.sql.adaptive.advisoryPartitionSizeInBytes","31457280b")
      .getOrCreate()

    println("======================================")
    println(sparkSession.conf.get("spark.sql.adaptive.enabled"))
    println(sparkSession.conf.get("spark.sql.adaptive.coalescePartitions.enabled"))
    println(sparkSession.conf.get("spark.sql.adaptive.advisoryPartitionSizeInBytes"))
    println("======================================")


    //读取JSON格式的数据，获取DataFrame
    val jsonDf = sparkSession.read.json("D:\\spark_json_1.dat")
    //创建临时表
    jsonDf.createOrReplaceTempView("t1")
    //执行SQL语句
    //注意：这个SQL中需要有可以产生Shuffle的语句，否则无法验证效果
    //在这里使用group by语句实现shuffle效果，并且还要注意尽量在group by后面多指定几个字段，否则shuffle阶段传输的数据量比较小，效果不明显
    val sql =
    """
      |select
      |	id,uid,lat,lnt,hots,title,status,
      |	topicId,end_time,watch_num,share_num,
      |	replay_url,replay_num,start_time,timestamp,area
      |from t1
      |group by id,uid,lat,lnt,hots,title,status,
      |		topicId,end_time,watch_num,share_num,
      |		replay_url,replay_num,start_time,timestamp,area
      |""".stripMargin
    sparkSession.sql(sql).write.format("json").save("hdfs://bigdata01:9000/aqe/coalesce_partition_"+System.currentTimeMillis())

    //让程序一直运行，这样本地的Spark任务界面就可以一直查看
    while (true){
      ;
    }
  }
}
