package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

/**
  * 设备标签
  */

object TagsDevice extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String, Int)]()

    // 处理参数
    // 设备标签不需要广播变量 和上面解题一样 因为读进来的数据本来就在内存中了，不用去外面调取
    // 但这种情况 用广播变量 有用嘛
    // 这什么情况，怎么都出来布尔值了，看agrs来源，没有什么问题啊
    val row = args(0).asInstanceOf[Row]

    // 设备操作系统
    val client = row.getAs[Int]("client")
    client match {
      case 1 => list :+= ("D00010001", 1)
      case 2 => list :+= ("D00010002", 1)
      case 3 => list :+= ("D00010003", 1)
      case _ => list :+= ("D00010004", 1)
    }

    // 设备联网方式
    val networkmannerid = row.getAs[String]("networkmannerid")
    networkmannerid match {
      case "WIFI" => list :+= ("D00020001", 1)
      case "4G" => list :+= ("D00020002", 1)
      case "3G" => list :+= ("D00020003", 1)
      case "2G" => list :+= ("D00020004", 1)
      case _ => list :+= ("D00020005", 1)
    }

    // 设备运营商方式
    val ispname = row.getAs[String]("ispname")
    ispname match {
      case "移动" => list :+= ("D00030001", 1)
      case "联通" => list :+= ("D00030002", 1)
      case "电信" => list :+= ("D00030003", 1)
      case _ => list :+= ("D00030004", 1)
    }
    list
  }
}
