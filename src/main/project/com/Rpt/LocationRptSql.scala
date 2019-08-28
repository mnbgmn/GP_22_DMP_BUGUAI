package com.Rpt

import org.apache.spark.sql.{DataFrame, SparkSession}

object LocationRptSql {
  def main(args: Array[String]): Unit = {

    // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
    val spark: SparkSession = SparkSession.builder().config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 设置压缩方式 使用Snappy方式进行压缩
      .config("spark.sql.parquet.compression.codec", "snappy")
      .appName(this.getClass.getName).master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    val df: DataFrame = spark.read.parquet("D://spark_project/yuanshuju_zhuang_parquet/part-00000-e6fedb22-be3c-48be-ae31-8a3a68a17d9d-c000.snappy.parquet")

    df.createOrReplaceTempView("log")

    //创建一张临时表
    spark.sql("create table if not exists tmp_log select REQUESTMODE,PROCESSNODE,ISEFFECTIVE,ISBILLING,ISBID,ISWIN,ADORDERID," +
      "provincename,cityname from log")

    //临时表展示数据
//    spark.sql("select * from tmp_log").show()

    //试验数据
//    spark.sql("select sum(case when REQUESTMODE = 1 and PROCESSNODE >= 1 then 1 else 0 end) `原始请求数` from tmp_log").show()

    spark.sql("select " +
      "provincename," +
      "cityname," +
      "sum(case when REQUESTMODE = 1 and PROCESSNODE >= 1 then 1 else 0 end) `原始请求数`," +
      "sum(case when REQUESTMODE = 1 and PROCESSNODE >= 2 then 1 else 0 end) `有效请求数`," +
      "sum(case when REQUESTMODE = 1 and PROCESSNODE = 3 then 1 else 0 end) `广告请求数`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISBID = 1 then 1 else 0 end) `参与竞价数`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 and ADORDERID != 0 then 1 else 0 end) `参与竞价数`," +
      "sum(case when REQUESTMODE = 2 and ISEFFECTIVE = 1 then 1 else 0 end) `展示数`," +
      "sum(case when REQUESTMODE = 3 and ISEFFECTIVE = 1 then 1 else 0 end) `点击数`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 then 1 else 0 end) `DSP广告消费`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 then 1 else 0 end) `DSP广告成本`" +
      "from " +
      "tmp_log " +
      "where provincename = '云南省' " +
      "group by provincename,cityname").show()
  }
}
