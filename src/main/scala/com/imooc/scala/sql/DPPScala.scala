package com.imooc.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 动态分区裁剪
 * Created by xuwei
 */
object DPPScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    //获取SparkSession，为了操作SparkSQL
    val sparkSession = SparkSession
      .builder()
      .appName("DPPScala")
      .config(conf)
      //关闭动态分区裁剪
      .config("spark.sql.optimizer.dynamicPartitionPruning.enabled","false")
      .getOrCreate()

    import sparkSession.implicits._

    /**
     * 创建一个表：t1
     * 1：表中有1000条数据
     * 2：表中有id和key这两个列，这两个列的值是一样的，起始值为0
     * 3：这个表示一个分区表，分区字段为key
     */
    sparkSession.range(1000)
      .select($"id",$"id".as("key"))
      .write
      .partitionBy("key")
      .mode("overwrite")
      .saveAsTable("t1")

    /**
     * 创建一个表：t2
     * 1：表中有10条数据
     * 2：表中有id和key这两个列，这两个列的值是一样的，起始值为0
     * 3：这个表是一个普通表
     */
    sparkSession.range(10)
      .select($"id",$"id".as("key"))
      .write
      .mode("overwrite")
      .saveAsTable("t2")

    val sql =
      """
        |select
        | t1.id,
        | t2.key
        | from t1 join t2
        |   on t1.key = t2.key
        |   and t2.id < 2
        |""".stripMargin

    sparkSession.sql(sql).write.format("json").save("hdfs://bigdata01:9000/dpp/dpp_"+System.currentTimeMillis())

    sparkSession.stop()
  }

}
