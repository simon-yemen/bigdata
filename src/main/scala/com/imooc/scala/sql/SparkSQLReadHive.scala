package com.imooc.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 在代码中通过SparkSQL操作Hive
 * Created by xuwei
 */
object SparkSQLReadHive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")

    //获取SparkSession，为了操作SparkSQL
    val sparkSession = SparkSession
      .builder()
      .appName("SparkSQLReadHive")
      .config(conf)
      //开启对Hive的支持，支持连接Hive的MetaStore、Hive的序列化、Hive的自定义函数
      .enableHiveSupport()
      .getOrCreate()
    //执行 sql查询
    //sparkSession.sql("select * from student_score").show()

    import sparkSession.sql
    sql("select * from student_score").show()

    sparkSession.stop()
  }

}
