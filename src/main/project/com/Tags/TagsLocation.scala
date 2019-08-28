package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsLocation extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String, Int)]()

    // 取值
    val row: Row = args(0).asInstanceOf[Row]

    // 获取省市
    val provincename = row.getAs[String]("provincename")
    val cityname = row.getAs[String]("cityname")

    // 打标签
    // 判断空值
//    if (StringUtils.isNotBlank(provincename)) {
//      provincename match {
//        case provincename => list :+= ("ZP" + provincename, 1)
//      }else if (StringUtils.isNoneBlank(cityname)) {  //这里不让这么用esle 直接if嘛？先不论对错 看看语法哪里错了
//        cityname match {
//          case cityname => list :+= ("ZC" + cityname, 1)
//        }
//      }
//    }
    //上面的只有一个值的 就不用模糊查询了 直接上标签即可
    if(StringUtils.isNotBlank(provincename)){
      list:+=("ZP"+provincename,1)
    }
    if(StringUtils.isNotBlank(cityname)){
      list:+=("ZC"+cityname,1)
    }
    list
  }
}