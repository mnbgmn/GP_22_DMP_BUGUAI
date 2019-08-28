package com.utils

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * 将字段文件数据，存储到redis中
  */

object App2Jedis {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 设置压缩方式 使用Snappy方式进行压缩
      .config("spark.sql.parquet.compression.codec", "snappy")
      .getOrCreate()

    // 读取字段文件
    val ds: Dataset[String] = spark.read.textFile("D://spark_project/app_dict.txt")

    import spark.implicits._
    // 读取字段文件
    ds.map(_.split("\t", -1))
      .filter(_.length >= 5).foreachPartition(arr => {
      val jedis = JedisConnectionPool.getConnection()
      arr.foreach(arr => {
        jedis.set(arr(4), arr(1))
      })
      jedis.close()
    })
    spark.stop()
  }
}
