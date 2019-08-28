package com.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object test2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("lcoal[2]")
      .getOrCreate()


  }
}
