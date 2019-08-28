package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * App 名称标签
  */

object TagsApp extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    // 处理参数类型
    // row是一条一条的数据吧，就是把args0传进来的数据转换为row，是这样的把？
    // 现在tagscontext那边传进来的就是处理过的原始数据然后和广播变量在这个类中进行对比，是这么理解吧
    // 还有个问题 怎么导入的数据和老师的类型不一样呢 哪里出错误了 看看args0的来源
    val row: Row = args(0).asInstanceOf[Row]
    val appmap = args(1).asInstanceOf[Broadcast[Map[String, String]]]

    // 获取APPid和APPname
    // 这里就是从刚刚传进来的row中的数据拿取id和name
    val appname: String = row.getAs[String]("appname")
    val appid: String = row.getAs[String]("appid")

    // 空值判断
    // 这个StringUtils这个包下有一个专门判断空值的方法isnotblank，这么理解对吧
    // 只要包导对了就可以，就跟java的equals类似，就是专门的一种方法
    if (StringUtils.isNotBlank(appname)) {
      list :+= ("APP" + appname, 1)
    } else if (StringUtils.isNotBlank(appid)) {
      // 这个getOrElse是做对比广播变量的值，具体是怎么用的？
      list :+= ("APP"+appmap.value.getOrElse(appid,appid),1)
    }
    list
  }
}