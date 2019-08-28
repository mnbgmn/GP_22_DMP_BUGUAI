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
object TagsConTextHbase {
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
    df.filter(TagUtils.OneUserId)
      // 所有的标签在这里实现
      .map(row => {
      // 1.db1为字典文件广播
      // 2.db2为停用词库广播
      // 取出用户ID
      val userId = TagUtils.getOneUserId(row)
      // 按照需求 通过row数据 打上所有标签
      // 1.广告位类型标签
      val adList: List[(String, Int)] = TagsAd.makeTags(row)
      // 2.App 名称标签
      val appList: List[(String, Int)] = TagsApp.makeTags(row, db1)
      // 3.关键字标签
      val keywordList = TagsKeyWord.makeTags(row, db2)
      // 4.设备标签
      val dvList = TagsDevice.makeTags(row)
      // 5.地域标签
      val loactionList = TagsLocation.makeTags(row)

      // 取值
      // ++是把list中的元素相加，那么:::呢
      (userId, adList ++ appList ++ keywordList ++ dvList ++ loactionList)
    })
      // 这个reducebykey怎么用不了呢
      // 按照相同key，对value进行操作
      .rdd.reduceByKey((list1, list2) =>
      // List(("lN插屏",1),("LN全屏",1),("ZC沈阳",1),("ZP河北",1)....)
      (list1 ::: list2)
        // List(("APP爱奇艺",List()))
        .groupBy(_._1)
        // mapvalues是什么意思
        .mapValues(_.foldLeft[Int](0)(_ + _._2))
        .toList
      //下面准备存hbase
    ).map {
      case (userid, userTag) => {
        val put = new Put(Bytes.toBytes(userid))
        // 处理下标签 如果不处理 就连在一块了
        val tags = userTag.map(t=>t._1+","+t._2).mkString(",")
        // 这里可以直接输入 但是太乱 所以改别的方式
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(s"2019"),Bytes.toBytes(tags))
        (new ImmutableBytesWritable(),put)
      }
    }
  }
}