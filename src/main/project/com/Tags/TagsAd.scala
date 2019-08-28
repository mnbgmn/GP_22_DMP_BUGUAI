package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row
import org.apache.commons.lang3.StringUtils

/**
  * 广告位类型标签
  */

object TagsAd extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]

    val adType = row.getAs[Int]("adspacetype")
    adType match {
      case v if v > 9 => list :+= ("LC" + v -> 1)
      case v if v <= 9 && v > 0 => list :+= ("LC0" + v,1)
    }
    val adName = row.getAs[String]("adspacetypename")

    if (StringUtils.isNoneBlank(adName)) {
      list :+= ("LN" + adName, 1)
    }
    list
  }
}
