package com.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 测试类
  */

object test {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("lcoal[2]")
      .getOrCreate()

    val list: List[String] = List("116.310003,39.991957")
    val rdd: RDD[String] = spark.sparkContext.makeRDD(list)

    val bs: RDD[String] = rdd.map(t => {
      val arr: Array[String] = t.split(",")
      AmapUtil.getBusinessFromAmap(arr(0).toDouble, arr(1).toDouble)
    })
    bs.foreach(println)
  }
}