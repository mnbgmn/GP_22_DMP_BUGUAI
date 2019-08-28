package com.Graphx

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 图计算案例
  */

object graph_test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 设置压缩方式 使用Snappy方式进行压缩
      .config("spark.sql.parquet.compression.codec", "snappy")
      .getOrCreate()

    // 构造点的集合
    val vertexRDD: RDD[(Long, (String, Int))] = spark.sparkContext.makeRDD(Seq(
      (1L, ("詹姆斯", 35)),
      (2L, ("霍华德", 34)),
      (6L, ("杜兰特", 31)),
      (9L, ("库里", 30)),
      (133L, ("哈登", 30)),
      (138L, ("席尔瓦", 36)),
      (16L, ("法尔考", 35)),
      (44L, ("内马尔", 27)),
      (21L, ("J罗", 28)),
      (5L, ("高斯林", 60)),
      (7L, ("奥德斯基", 55)),
      (158L, ("码云", 55))
    ))

    //构造边的集合
    val egde: RDD[Edge[Int]] = spark.sparkContext.makeRDD(Seq(
      //Edge是spark的图计算的方法，1.初始点 2.结束点 3.点的属性
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 138L, 0),
      Edge(16L, 138L, 0),
      Edge(44l, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))

    // 构建图
    // 直接调用其中的构造方法
    val graph = Graph(vertexRDD,egde)
    // 取出每个边上的最大顶点
    // 就是唯一的顶点，然后从1开始，最小的就是顶点
    val vertices = graph.connectedComponents().vertices
//    vertices.foreach(println)
    vertices.join(vertexRDD).map{
      case(userId,(conId,(name,age)))=>{
        // conID是公共点
        (conId,List(name,age))
      }
    }.reduceByKey(_++_).foreach(println)
  }
}