package com.Rpt

import com.utils.RptUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 媒体指标
  */

object APPRpt {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 设置压缩方式 使用Snappy方式进行压缩
      .config("spark.sql.parquet.compression.codec", "snappy")
      .getOrCreate()

    val df: DataFrame = spark.read.parquet("D://spark_project/yuanshuju_zhuang_parquet/part-00000-e6fedb22-be3c-48be-ae31-8a3a68a17d9d-c000.snappy.parquet")

    val ds1: Dataset[String] = spark.read.textFile("D://spark_project/app_dict.txt")

    val rdd2: RDD[Array[String]] = ds1.rdd.map(x => {
      x.split("\t")
    }).filter(x => {
      x.length > 4
    })

    val map1: Map[String, String] = rdd2.map(x => {
      x(1)
      x(4)
      (x(1), x(4))
    }).collect().toMap

    // 创建广播变量
    val bd1: Broadcast[Map[String, String]] = spark.sparkContext.broadcast(map1)

    import spark.implicits._

    //问这个.add的问题

    val ds: Dataset[((String), List[Double])] = df.map(row => {
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")

      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")

      //运营商
      val appid: String = row.getAs[String]("appid")
      val appname:String = row.getAs[String]("appname")

      val name: String = bd1.value.getOrElse(appid,appname)

      val first: List[Double] = RptUtils.request(requestmode, processnode)
      val second: List[Double] = RptUtils.click(requestmode, iseffective)
      val thirdly: List[Double] = RptUtils.bidding(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

      (name, first ++ second ++ thirdly)
    })

    //    ds.collect().foreach(println(_))
        val rdd1: RDD[(String, List[Double])] = ds.rdd.reduceByKey((x, y) => {
          x.zip(y).map(x => x._1 + x._2)
        })

        rdd1.collect().foreach(println(_))
  }
}