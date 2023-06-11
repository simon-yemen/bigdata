package com.imooc.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 使用SparkSQL向Hive表中写入数据
 * Created by xuwei
 */
object SparkSQLWriteHive_3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")

    //获取SparkSession，为了操作SparkSQL
    val sparkSession = SparkSession
      .builder()
      .appName("SparkSQLWriteHive_3")
      .config(conf)
      //开启对Hive的支持，支持连接Hive的MetaStore、Hive的序列化、Hive的自定义函数
      .enableHiveSupport()
      .getOrCreate()

    import sparkSession.sql

    //3：SparkSQL语句
    /**
     * 分为两种情况：表不存在和表存在
     * 1：表不存在，使用create table as select
     * 2：表存在，使用insert into select
     */
    //表不存在
    /*sql(
      """
        |create table student_score_bak
        |as
        |select * from student_score
        |""".stripMargin)*/

    //表存在
    sql(
      """
        |create table if not exists student_score_bak(
        | id int,
        | name string,
        | sub string,
        | score int
        |)using hive
        | OPTIONS(
        |   fileFormat 'textfile',
        |   fieldDelim ','
        |   )
        |""".stripMargin)

    sql(
      """
        |insert into student_score_bak
        |select * from student_score
        |""".stripMargin)


    sparkSession.stop()
  }

}
