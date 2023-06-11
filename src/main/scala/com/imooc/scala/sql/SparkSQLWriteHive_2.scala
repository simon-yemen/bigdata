package com.imooc.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 使用saveAsTable()向Hive表中写入数据
 * Created by xuwei
 */
object SparkSQLWriteHive_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")

    System.setProperty("hadoop.home.dir","D:\\hadoop-3.2.0")
    //获取SparkSession，为了操作SparkSQL
    val sparkSession = SparkSession
      .builder()
      .appName("SparkSQLWriteHive_2")
      .config(conf)
      .config("spark.sql.warehouse.dir","hdfs://bigdata01:9000/user/hive/warehouse")
      //开启对Hive的支持，支持连接Hive的MetaStore、Hive的序列化、Hive的自定义函数
      .enableHiveSupport()
      .getOrCreate()

    import sparkSession.sql
    //查询数据
    val resDf = sql("select * from student_score")
    //2：saveAsTable()
    /**
     * 分为两种情况，表不存在和表存在
     * 1：表不存在，则会根据DataFrame中的Schema自动创建目标表并写入数据
     * 2：表存在
     *  2.1：如果mode=append，当DataFrame中的Schema和表中的Schema相同(字段顺序可以不同)，则执行追加操作。
     *  当DataFrame中的Schema和表中的Schema不相同，则报错。
     *  2.2：如果mode=overwrite，当DataFrame中的Schema和表中的Schema相同(字段顺序可以不同)，则直接覆盖。
     *  当DataFrame中的Schema和表中的Schema不相同，则会删除之前的表，然后按照DataFrame中的Schema重新创建表并写入数据。
     */

    //表不存在
    /*resDf.write
      //指定数据写入格式append：追加。overwrite：覆盖。
      .mode("overwrite")
      //这里需要指定数据格式：parquet, orc, avro(需要添加外部依赖), json, csv, text。不指定的话默认是parquet格式。
      //注意：text数据格式在这里不支持int数据类型
      //针对普通文本文件数据格式(json、csv)，默认创建的Hive表示SequenceFile格式的，无法读取生成的普通文件，不要使用这种格式。
      //parquet、orc数据格式可以正常使用
      .format("parquet")
      .saveAsTable("student_score_bak")*/

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
        |   fileFormat 'parquet'
        |   )
        |""".stripMargin)
    resDf.write
      //指定数据写入格式append：追加。overwrite：覆盖。
      .mode("overwrite")
      //这里需要指定数据格式：parquet, orc, avro(需要添加外部依赖), json, csv, text。不指定的话默认是parquet格式。
      //注意：text数据格式在这里不支持int数据类型
      //针对已存在的表，当mode为append时，这里必须指定为hive。
      //针对已存在的表，当mode为overwrite时：
      //  这里如果指定为hive，则会生成默认数据存储格式(TextFile)的Hive表
      //  这里如果指定为普通文本文件数据格式(json、csv)，默认创建的Hive表是SequenceFile格式的，无法读取生成的普通文件，不需要使用这种方式
      //parquet、orc数据格式可以正常使用
      .format("parquet")
      .saveAsTable("student_score_bak")


    sparkSession.stop()
  }

}
