package com.Rpt

import com.utils.RptUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 3.2.1地域分布
  */

object LocationRptCore {
  def main(args: Array[String]): Unit = {
    //判断路径
    //    if(args.length  != 2){
    //      println("输入路径参数有误，重写")
    //      sys.exit()
    //    }

    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[2]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 设置压缩方式 使用Snappy方式进行压缩
      .config("spark.sql.parquet.compression.codec", "snappy")
      .getOrCreate()

    val df: DataFrame = spark.read.parquet("D://spark_project/yuanshuju_zhuang_parquet/part-00000-e6fedb22-be3c-48be-ae31-8a3a68a17d9d-c000.snappy.parquet")

    //    val rdd1: RDD[Row] = df.rdd
    //    为什么不转成这样的呢
    //df带表头，可以取到字段
    //    rdd1.map(x=>{
    //      val requestmore
    //    })

    import spark.implicits._

    val ds: Dataset[((String, String), List[Double])] = df.map(row => {
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")

      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")

      //省，市
      val provincename: String = row.getAs[String]("provincename")
      val cityname: String = row.getAs[String]("cityname")

      val first: List[Double] = RptUtils.request(requestmode, processnode)
      val second: List[Double] = RptUtils.click(requestmode, iseffective)
      val thirdly: List[Double] = RptUtils.bidding(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

      ((provincename, cityname), first ++ second ++ thirdly)
    })

    //    ds.collect().foreach(println(_))

    //    ds.show()
    val rdd1: RDD[((String, String), List[Double])] = ds.rdd.reduceByKey((x, y) => {
      x.zip(y).map(x => x._1 + x._2)
    })
    rdd1.collect().foreach(println(_))
  }
}