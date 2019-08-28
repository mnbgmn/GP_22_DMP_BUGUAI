package com.kaoshi

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ListBuffer

object ApUtil2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()

    // 读取数据
    val ds: Dataset[String] = spark.read.textFile("D://kaoshi/json.txt")
    val arr: Array[String] = ds.collect()

    val arr1 = arr.map(x => {

      val jsonparse = JSON.parseObject(x)

      // 判断状态是否成功
      val status: Int = jsonparse.getIntValue("status")

      if (status == 0) return ""

      // 接下来解析内部json串，判断每个key的value都不能为空
      val regecodeJson: JSONObject = jsonparse.getJSONObject("regeocode")
      if (regecodeJson == null || regecodeJson.keySet().isEmpty) return ""

      //pois-Array类型的
      //所以要用到getJSONArray
      val poisArray: JSONArray = regecodeJson.getJSONArray("pois")
      if (poisArray == null || poisArray.isEmpty) return ""

      // 创建集合 保存数据
      var list: List[(String, Int)] = List[(String, Int)]()

      // 循环输出
      for (item <- poisArray.toArray) {
        if (item.isInstanceOf[JSONObject]) {
          val json: JSONObject = item.asInstanceOf[JSONObject]
          val arrType: Array[String] = json.getString("type").split(";")
          arrType.foreach(x => {
            list :+= (x, 1)
          })
        }
      }
      list
    })
    println(arr1.reduce(_:::_).groupBy(_._1).mapValues(_.size).toBuffer)
  }
}