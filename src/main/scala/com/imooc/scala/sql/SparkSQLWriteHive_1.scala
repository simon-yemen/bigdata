package com.imooc.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 使用insertInto()向Hive表中写入数据
 * Created by xuwei
 */
object SparkSQLWriteHive_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")

    System.setProperty("hadoop.home.dir","D:\\hadoop-3.2.0")
    //获取SparkSession，为了操作SparkSQL
    val sparkSession = SparkSession
      .builder()
      .appName("SparkSQLWriteHive_1")
      .config(conf)
      //开启对Hive的支持，支持连接Hive的MetaStore、Hive的序列化、Hive的自定义函数
      .enableHiveSupport()
      .getOrCreate()

    import sparkSession.sql
    //查询数据
    val resDf = sql("select * from student_score")
    //1：insertInto()
    /**
     * 需要满足2个条件：
     * 1：写入的Hive表是存在的(可以在Hive中建表，或者通过SparkSQL建表，官方建议在Hive中建表！)
     * 2：DataFrame数据的Schema结构顺序和写入的Hive表的Schema结构顺序是一样的
     *
     * 注意：其实针对这种情况，可以考虑直接输出HDFS文件，只需要将数据文件写入到Hive表对应的HDFS目录下即可
     */

    /**
     * 官方建议提前在Hive中创建表，在SparkSQL中直接使用
     * 注意：通过SparkSQL创建Hive表的时候，如果想要指定存储格式等参数(默认是TextFile)，则必须要使用using hive。
     * 这样指定是无效的：create table t1(id int) OPTIONS(fileFormat 'parquet')
     * 这样才是有效的：create table t1(id int) using hive OPTIONS(fileFormat 'parquet')
     * create table t1(id int) 等于 create table t1(id int) using hive OPTIONS(fileFormat 'textfile')
     *
     * fileFormat支持6种参数：'sequencefile', 'rcfile', 'orc', 'parquet', 'textfile' and 'avro
     */
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

    resDf.write
      //指定数据写入格式append：追加。overwrite：覆盖。
      .mode("overwrite")
      //这里需要指定数据格式：parquet, orc, avro(需要添加外部依赖), json, csv, text。不指定的话默认是parquet格式。
      //针对insertInto而言，因为它是向一个已存在的表中写入数据，所以format参数和option参数会被忽略。
      //.format("text")
      .insertInto("student_score_bak")

    sparkSession.stop()
  }

}
