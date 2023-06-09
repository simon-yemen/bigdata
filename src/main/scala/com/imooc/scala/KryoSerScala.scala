package com.imooc.scala

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求：Kryo序列化的使用
 * 使用kryo序列化可以不对对象进行注册，但是序列化后的数据量会任然很大，介于java原生序列化和kryo注册对象方式之间
 * Created by xuwei
 */
object KryoSerScala {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("KryoSerScala")
      .setMaster("local")
      //指定使用kryo序列化机制，注意：如果使用了registerKryoClasses，其实这一行设置是可以省略的
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Person]))//注册自定义的数据类型
    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(Array("hello you","hello me"))
    val wordsRDD = dataRDD.flatMap(_.split(" "))
    val personRDD = wordsRDD.map(word=>Person(word,18)).persist(StorageLevel.MEMORY_ONLY_SER)
    personRDD.foreach(println(_))

    while(true){
      ;
    }
  }

}
case class Person(name: String,age: Int) extends Serializable
