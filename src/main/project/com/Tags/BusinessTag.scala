package com.Tags

import ch.hsr.geohash.GeoHash
import com.utils.{AmapUtil, JedisConnectionPool, Tag, Utils2Type}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * // 地图的请求这么贵，怎么办
  * // 不要多次访问，已经访问的存起来，只有新的，才去访问
  * // 把这里的商圈进行缓存，每次访问地图的时候，现在访问缓存，如果缓存为空，再去地图访问
  *
  * // redis里直接拼接的经纬度是kv 是固定的 现在要的是一个范围的经纬度
  * // 西三旗好程序员22班定位到22班 然后西三旗好程序员 就定位的范围大
  * // 给用户推送广告的时候 需要推荐附近的 不能仅仅的是一个点 有范围的
  *
  * // 这里有一个算法 k要用ju哈希算法来设计 通过经纬度能算范围型的商圈
  * // 两点：1、算法（k-ju哈希算法，v-对应商圈） 2、缓存（商圈）
  */

/**
  * 商圈标签
  */

object BusinessTag extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()
    // 解析参数
    val row: Row = args(0).asInstanceOf[Row]
    // 获取经纬度（要中国的经纬度 过滤一下，要中国的就可以）
    // 中国的经纬度范围大约为：纬度3.86~53.55，经度73.66~135.05, 不在范围类的数据可需要处理
    //    if (row.getAs[String]("long").toDouble >= 73 && row.getAs[String]("long").toDouble <= 135
    //      && row.getAs[String]("lat").toDouble >= 3 && row.getAs[String]("lat").toDouble <= 54) {
    // 上面if这样写的，出来报错，有空，所以要判断，导入有判断 的包来判断
    val long = row.getAs[String]("long")
    val lat = row.getAs[String]("lat")
    // 获取经纬度，过滤经纬度
    if (Utils2Type.toDouble(long) >= 73.0 &&
      Utils2Type.toDouble(long) <= 135.0 &&
      Utils2Type.toDouble(lat) >= 3.0 &&
      Utils2Type.toDouble(lat) <= 54.0) {
      // 然后这里先去数据库获取商圈
      // 不新建util了 直接在这里写 正常来说 应该建立一个工具类
      val business = getBusiness(long.toDouble, lat.toDouble)
      // 判断缓存中是否有此商圈
      if (StringUtils.isNotBlank(business)) {
        val lines = business.split(",")
        lines.foreach(f => list :+= (f, 1))
      }
      //      list:+=(business,1)
    }
    list
  }

  /**
    * 获取商圈信息
    */
  def getBusiness(long:Double,lat:Double) = {

    // 获取k 要先去设置GeoHash
    // 有pom
    // 8位是很近的，可以自己选择
    val geohash: String = GeoHash.geoHashStringWithCharacterPrecision(lat, long, 8)
    // 去数据库查询
    var business = redis_queryBusiness(geohash)
    // 判断商圈是否为空
    // 这里是否能用StringUtils的方法啊
    if (business == null || business == 0) {
      // 通过经纬度获取商圈
      business = AmapUtil.getBusinessFromAmap(long.toDouble, lat.toDouble)
      // 如果调用高德地图解析商圈 那么需要将此次商圈存入redis
      redis_inserBusiness(geohash, business)
    }
    business
  }

  /**
    * 获取商圈信息
    */
  def redis_queryBusiness(geohash: String) = {
    val jedis = JedisConnectionPool.getConnection()
    val business = jedis.get(geohash)
    jedis.close()
    business
  }

  /**
    * 存储商圈到redis
    */

  def redis_inserBusiness(geohash: String, business: String) = {
    val jedis = JedisConnectionPool.getConnection()
    jedis.set(geohash, business)
    jedis.close()
  }
}