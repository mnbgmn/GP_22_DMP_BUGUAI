package com.Tags

import com.typesafe.config.ConfigFactory
import com.utils.TagUtils
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 上下文标签
  */
/**
  * val conf: SparkConf = new SparkConf().setAppName("kaoshi2").setMaster("local[2]")
  * val sc: SparkContext = new SparkContext(conf)
  * val linesRDD: RDD[Array[String]] = sc.textFile("dir/test.txt").map(_.split(" "))
  * val sssRDD: RDD[sss] = linesRDD.map(x=>sss(x(0).toInt,x(1),x(2)))
  * val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  * 对比一下创建方式
  * //两个执行入口
  */
object TagsConTextHbase1 {
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

    // todo 调用Hbase API
    // 加载配置文件
    val load = ConfigFactory.load()
    // 加载表
    val hbaseTableName = load.getString("hbase.TableName")
    // 创建Hadoop任务
    // 只要是sc调用的，spark价格sparkContext就转了
    val configuration = spark.sparkContext.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum", "192.168.183.17")
    configuration.set("hbase.zookeeper.property.clientPort","2181")
    // 穿件HBASEConnection
    val hbconn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbconn.getAdmin
    // 判断表是否可用
    if (!hbadmin.tableExists(TableName.valueOf(hbaseTableName))) {
      // 创建表操作
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      // 这个创建的列簇
      val descriptor = new HColumnDescriptor("tags")
      tableDescriptor.addFamily(descriptor)
      hbadmin.createTable(tableDescriptor)
      hbadmin.close()
      hbconn.close()
    }
    // 创建相关的 Job
    val jobconf = new JobConf(configuration)

    // 指定输出类型和表
    jobconf.setOutputFormat(classOf[TableOutputFormat])

    // 指定下输出的表
    jobconf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)

    import spark.implicits._

    // 读取文件
    val df: DataFrame = spark.read.parquet(inputPath)

    //读取字典文件 并处理
    val ds: Dataset[String] = spark.read.textFile(dirPath)
    //    val map: collection.Map[String, String] = ds.rdd.map(x=>x.split("\t"))
    //      .filter(x=>x.length>4).map(x=>(x(1),x(4))).collectAsMap()
    //上下这两个有什么区别
    val map1: Map[String, String] = ds.rdd.map(x => x.split("\t"))
      .filter(x => x.length > 4).map(x => (x(1), x(4))).collect().toMap

    // 将处理好的字典文件广播
    val db1: Broadcast[Map[String, String]] = spark.sparkContext.broadcast(map1)

    // 获取停用词库
    //    val stopds: Dataset[String] = spark.read.textFile(stopPath).map(x=>x(0))
    //上面这样为什么不行呢
    //停用词库，为什么输出一个这个呢，ds
    //问题，rdd调用的是算子，ds调用的是scala的方法吗？那么这里就是一个二元祖嘛
    //看过笔记了，应该是rdd,df,ds都是分布式弹性数据集，所以调用的都是算子，这么说对吗？就是rdd的算子，ds都可以用？
    //就是不懂为什么_,0形成一个二元组
    val map2: Map[String, Int] = spark.read.textFile(stopPath).map((_, 0)).collect().toMap
    //这样没collectAsMap()的方法，考虑加conText就有了
    //    spark.sparkContext.textFile(stopPath).map((_,0)).collectAsMap()
    // 将处理好的停用词库广播
    val db2: Broadcast[Map[String, Int]] = spark.sparkContext.broadcast(map2)

    // 过滤符合ID的条件 //调用一个字符串就可以过滤嘛？这个filter能这么用嘛？看笔记，不是很明白
    val baseRDD = df.filter(TagUtils.OneUserId)
      // 所有的标签在这里实现
      .map(row => {
      val userList: List[String] = TagUtils.getAllUserId(row)
      (userList, row)
    })

    // 构建点集合
    // 这里为什么是ds类型 而老师的是rdd呢
    val vertiesRDD: Dataset[(Long, List[(String, Int)])] = baseRDD.flatMap(tp => {
      val row = tp._2
      // 所有标签
      val adList = TagsAd.makeTags(row)
      val appList = TagsApp.makeTags(row, db2)
      val keywordList = TagsKeyWord.makeTags(row, db2)
      val dvList = TagsDevice.makeTags(row)
      val loactionList = TagsLocation.makeTags(row)
      val business = BusinessTag.makeTags(row)
      val AllTag = adList ++ appList ++ keywordList ++ dvList ++ loactionList ++ business
      // List((String,Int))
      // 保证其中一个点携带者所有标签，同时也保留所有userId
      val VD = tp._1.map((_, 0)) ++ AllTag
      // 处理所有的点集合
      tp._1.map(uId => {
        // 保证一个点携带标签 (uid,vd),(uid,list()),(uid,list())
        if (tp._1.head.equals(uId)) {
          (uId.hashCode.toLong, VD)
        } else {
          (uId.hashCode.toLong, List.empty)
        }
      })
    })

    // vertiesRDD.take(50).foreach(println)
    // 构建边的集合
    val edges: Dataset[Edge[Int]] = baseRDD.flatMap(tp => {
      // A B C : A->B A->C
      tp._1.map(uId => Edge(tp._1.head.hashCode, uId.hashCode, 0))
    })

    //edges.take(20).foreach(println)
    // 构建图
    // 因为上面是ds，老师直接是Graph(vertiesRDD,edges) 这里直接.rdd 这样对嘛
    val graph = Graph(vertiesRDD.rdd,edges.rdd)
    // 取出顶点 使用的是图计算中的连通图算法
    val vertices = graph.connectedComponents().vertices
    // 处理所有的标签和id
    vertices.join(vertiesRDD.rdd).map{
      case (uId,(conId,tagsAll))=>(conId,tagsAll)
    }.reduceByKey((list1,list2)=>{
      // 聚合所有的标签
      (list1++list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    })
      .take(20).foreach(println)

    spark.stop()
  }
}