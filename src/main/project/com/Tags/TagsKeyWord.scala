package com.Tags

import com.utils.Tag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * 关键字标签
  */

object TagsKeyWord extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
//    val list = List[(String, Int)]()
    var list = List[(String,Int)]()
    // 上面两个有啥区别的，就多个空格，咋就不对了

    // 这个标签的需求还是不懂什么意思
    // 这个也是row的类型
    val row = args(0).asInstanceOf[Row]
    val stopword = args(1).asInstanceOf[Broadcast[Map[String, Int]]]

    // 获取关键字打标签
    // 看原数据还要分开
    val kwds = row.getAs[String]("keywords").split("\\|")

    // 按照过滤条件进行过滤数据
    kwds.filter(word => {
      word.length >= 3 && word.length <= 8 && !stopword.value.contains(word)
    })
      .foreach(word => list :+= ("K" + word, 1))
    list
  }
}