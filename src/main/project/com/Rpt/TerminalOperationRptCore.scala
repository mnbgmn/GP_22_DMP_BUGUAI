package com.Rpt

import com.utils.RptUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 终端设备_运营
  */

object TerminalOperationRptCore {
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
      val ispname: String = row.getAs[String]("ispname")

      val first: List[Double] = RptUtils.request(requestmode, processnode)
      val second: List[Double] = RptUtils.click(requestmode, iseffective)
      val thirdly: List[Double] = RptUtils.bidding(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

      ((ispname), first ++ second ++ thirdly)
    })

        ds.collect().foreach(println(_))

    val rdd1: RDD[((String), List[Double])] = ds.rdd.reduceByKey((x, y) => {
      x.zip(y).map(x => x._1 + x._2)
    })

    rdd1.collect().foreach(println(_))
  }
}
