package com.utils

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable.ListBuffer

/**
  * 商圈解析工具
  */

object AmapUtil {

  // 获取高德地图商圈信息
  def getBusinessFromAmap(long: Double, lat: Double): String = {
    // 服务器示例

    val location = long + "," + lat
    val urlStr = ""
    // 调用请求
    val jsonstr = HttpUtil.get(urlStr)
    // 解析json串
    // 也就是求得json的数据信息
    // 因为这了是大json套小json，一直嵌套
    // 这里有一个状态码 如果是0没有数据请求失败 如果是1有数据
    // status在返回结果参数有说明
    val jsonparse: JSONObject = JSON.parseObject(jsonstr)
    // 判断状态是否成功
    // 这个方法，直接通过取出的状态码，状态码在第一位
    val status: Int = jsonparse.getIntValue("status")
    if(status == 0) return ""
    // 接下来解析内部json串，判断每个key的value都不能为空
    // 如果看不懂，就去看文档结果参数说明，说明很清晰
    val regecodeJson: JSONObject = jsonparse.getJSONObject("regeocode")
    // 如果regecodeJson为空，或者regecodeJson里面的keyset值也是空的，就返回空，结束
    if(regecodeJson == null || regecodeJson.keySet().isEmpty) return ""
    // 继续解析
    val addressComponentJson: JSONObject = regecodeJson.getJSONObject("addressComponent")
    if(addressComponentJson == null || addressComponentJson.keySet().isEmpty) return ""

    // 在json中有[]，因为是数据，就要用getJSONArray，别的大括号就是getJSONObject

    val businessAressArray: JSONArray = addressComponentJson.getJSONArray("businessAress")
    if(businessAressArray == null || businessAressArray.isEmpty) return ""
    // 创建集合 保存数据
    val buffer: ListBuffer[String] = collection.mutable.ListBuffer[String]()
    // 循环输出
    for(item <- businessAressArray.toArray()){
      // 判断这个值是否是ali的JSon什么的
      if(item.isInstanceOf[JSONObject]){
        //这里在强转是什么意思？
        val json: JSONObject = item.asInstanceOf[JSONObject]
        buffer.append(json.getString("name"))
          //为什么没有json的
      }
    }
    buffer.mkString("")
  }
}