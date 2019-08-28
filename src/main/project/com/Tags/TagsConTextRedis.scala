package com.Tags

import com.utils.{JedisConnectionPool, TagUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object TagsConTextRedis {
  def main(args: Array[String]): Unit = {
    // 判断路径
    if (args.length != 3) {
      println("目录不匹配，退出程序")
      sys.exit()
    }
    val Array(inputPath, dirPath, stopPath) = args

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 设置压缩方式 使用Snappy方式进行压缩
      .config("spark.sql.parquet.compression.codec", "snappy")
      .getOrCreate()

    import spark.implicits._

    // 读取文件
    val df: DataFrame = spark.read.parquet(inputPath)

    // 字典文件用redis进行读取 不用广播

    // 获取停用词库
    val map2: Map[String, Int] = spark.read.textFile(stopPath).map((_, 0)).collect().toMap
    val db2: Broadcast[Map[String, Int]] = spark.sparkContext.broadcast(map2)

    df.filter(TagUtils.OneUserId)
      // 接下来所有的标签都在内部实现
      .mapPartitions(row=>{
      val jedis = JedisConnectionPool.getConnection()
      var list = List[(String,List[(String,Int)])]()
      row.map(row=>{
        // 取出用户Id
        val userId = TagUtils.getOneUserId(row)
        // 接下来通过row数据 打上 所有标签（按照需求）
        val adList = TagsAd.makeTags(row)
        val appList = TagsApp.makeTags(row,jedis)
        val keywordList = TagsKeyWord.makeTags(row,db2)
        val dvList = TagsDevice.makeTags(row)
        val loactionList = TagsLocation.makeTags(row)
        list:+=(userId,adList++appList++keywordList++dvList++loactionList)
      })
      jedis.close()
      list.iterator
    })
      .rdd.reduceByKey((list1,list2)=>
        // List(("lN插屏",1),("LN全屏",1),("ZC沈阳",1),("ZP河北",1)....)
        (list1:::list2)
          // List(("APP爱奇艺",List()))
          .groupBy(_._1)
          .mapValues(_.foldLeft[Int](0)(_+_._2))
          .toList
      ).foreach(println)
  }
}
