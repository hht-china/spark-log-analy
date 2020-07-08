package com.hht.stream

import java.sql.Date

import com.hht.commons.conf.ConfigurationManager
import com.hht.commons.utils.DateUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object AdClickRealTimeStat {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("streamingRecommendingSystem").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext
    //创建StreamingContext
    val ssc = new StreamingContext(sc, Seconds(5))

    // 设置检查点目录
    ssc.checkpoint("./streaming_checkpoint")

    // 获取Kafka配置
    val broker_list = ConfigurationManager.config.getString("kafka.broker.list")
    val topics = ConfigurationManager.config.getString("kafka.topics")

    // kafka消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> broker_list, //用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> "spark-log-consumer-group",
      /*auto.offset.reset
           latest:  先去Zookeeper获取offset，如果有，直接使用，如果没有，从最新的数据开始消费；
           earlist :  先去Zookeeper获取offset，如果有，直接使用，如果没有，从最开始的数据开始消费
           none: 先去Zookeeper获取offset，如果有，直接使用，如果没有，直接报错
       */
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    /* 创建DStream，返回接收到的输入数据，我们从kafka拿的数据都是一个msg，是k-v结构，v就是我们发送的log日志
          LocationStrategies：根据给定的主题和集群地址创建consumer
                 LocationStrategies.PreferConsistent：持续的在所有Executor之间均匀的分配
          ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
                 ConsumerStrategies.Subscribe：订阅一系列主题
     */
    val adRealTimeLogDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topics), kafkaParam))


    //取出了DSream里面每一条数据的value值
    val adRealTimeValueDStream: DStream[String] = adRealTimeLogDStream.map(line => line.value())

    //adRealTimeValueDStream.print()

    // 用于Kafka Stream的线程非安全问题，重新分区切断血统
    adRealTimeValueDStream.repartition(400)

    val filterdDStream: DStream[String] = adRealTimeValueDStream.transform {
      logRDD => {
        //先过滤掉已经在黑名单里的数据，已经在黑名单了，我们自然不用再管它了
        val blacklistArray: Array[AdBlacklist] = AdBlacklistDAO.findAll()
        val blackUserIdArray: Array[Long] = blacklistArray.map(adBlacklist => adBlacklist.userid)

        val filterdLogRDD: RDD[String] = logRDD.filter {
          case log => {
            val logArray: Array[String] = log.split(" ")
            val userid: String = logArray(3)
            val isBlackUserLog: Boolean = blackUserIdArray.contains(userid)
            //要返回的是不是黑名单用户的日志，别搞错了啊
            !isBlackUserLog
          }
        }
        filterdLogRDD
      }
    }

    /*
    业务功能一：生成动态黑名单
     */
    generateBlackList(filterdDStream)

    /*
     业务功能二：计算广告点击流量实时统计结果
         维度：每天每个省份的每个城市的每个广告
     */
    val ad2CountDStream: DStream[(String, Long)] = generateRealTimeAdClickCount(filterdDStream)

    // ad2CountDStream.print()

    /*
      业务功能三：每天每个省份Top3热门广告
         说白了还是topN
     */
    generateTop3AdByDateAndProvice(sparkSession, ad2CountDStream)

    /*
    业务功能四：最近一小时广告点击量实时统计
         最近一小时------肯定是窗口算子了
         其实就是每分钟的流量统计图，前端可以展示出来
     */
    calculateAdClickCountByWindow(adRealTimeValueDStream)

    ssc.start()
    ssc.awaitTermination()
  }

  def calculateAdClickCountByWindow(adRealTimeValueDStream: DStream[String]) = {
    val timeAd2OneDStream: DStream[(String, Long)] = adRealTimeValueDStream.map {
      case consumerRecord =>
        val logSplited = consumerRecord.split(" ")
        val timeMinute = DateUtils.formatTimeMinute(new Date(logSplited(0).toLong))
        val adid = logSplited(4).toLong

        (timeMinute + "_" + adid, 1L)
    }

    val timeAd2CountDStream: DStream[(String, Long)] = timeAd2OneDStream.reduceByKeyAndWindow((a: Long, b: Long) => (a + b), Minutes(60L), Seconds(10L))

    timeAd2CountDStream.foreachRDD { rdd =>
      rdd.foreachPartition { items =>
        //保存到数据库
        val adClickTrends = ArrayBuffer[AdClickTrend]()
        for (item <- items) {
          val keySplited = item._1.split("_")
          // yyyyMMddHHmm
          val dateMinute = keySplited(0)
          val adid = keySplited(1).toLong
          val clickCount = item._2

          val date = DateUtils.formatDate(DateUtils.parseDateKey(dateMinute.substring(0, 8)))
          val hour = dateMinute.substring(8, 10)
          val minute = dateMinute.substring(10)

          adClickTrends += AdClickTrend(date, hour, minute, adid, clickCount)
        }
        AdClickTrendDAO.updateBatch(adClickTrends.toArray)
      }
    }
  }

  def generateTop3AdByDateAndProvice(sparkSession: SparkSession, ad2CountDStream: DStream[(String, Long)]) = {
    //有一个多余的城市信息，我们需要去掉并对省份聚合
    val rowsDStream: DStream[Row] = ad2CountDStream.transform {
      rdd =>
        val adDatePrivince2CountRDD: RDD[(String, Long)] = rdd.map {
          case (key, count) =>
            val keySplitArr: Array[String] = key.split("_")
            val date = keySplitArr(0)
            val province = keySplitArr(1)
            val adid = keySplitArr(3).toLong
            val clickCount = count

            val newKey = date + "_" + province + "_" + adid
            (newKey, count)
        }
        val adDatePrivince2GroupCountRDD: RDD[(String, Long)] = adDatePrivince2CountRDD.reduceByKey(_ + _)

        val rowsRDD: RDD[(String, String, Long, Long)] = adDatePrivince2GroupCountRDD.map {
          case (keyString, count) =>
            val keySplited = keyString.split("_")
            val datekey = keySplited(0)
            val province = keySplited(1)
            val adid = keySplited(2).toLong
            val clickCount = count

            val date = DateUtils.formatDate(DateUtils.parseDateKey(datekey))

            (date, province, adid, clickCount)
        }


        import sparkSession.implicits._
        rowsRDD.toDF("date", "province", "ad_id", "click_count").createOrReplaceTempView("ad_date_province_count")
        val sql = "SELECT t.date,t.province,t.ad_id,t.click_count " +
          "FROM " +
          "(SELECT date,province,ad_id,click_count,ROW_NUMBER() OVER(PARTITION BY province SORT BY click_count DESC) rank " +
          "FROM ad_date_province_count)  t " +
          "WHERE rank <=3"
        val provinceTop3AdDF: DataFrame = sparkSession.sql(sql)
        provinceTop3AdDF.rdd
    }
    rowsDStream.foreachRDD(
      rdd =>
        rdd.foreachPartition {
          items =>
            // 插入数据库
            val adProvinceTop3s = ArrayBuffer[AdProvinceTop3]()

            for (item <- items) {
              val date = item.getString(0)
              val province = item.getString(1)
              val adid = item.getLong(2)
              val clickCount = item.getLong(3)
              adProvinceTop3s += AdProvinceTop3(date, province, adid, clickCount)
            }
            AdProvinceTop3DAO.updateBatch(adProvinceTop3s.toArray)
        }
    )
  }

  def generateRealTimeAdClickCount(filterdDStream: DStream[String]): DStream[(String, Long)] = {

    val updateCountByAdDStream: DStream[(String, Long)] = {
      val ad2OneDStream: DStream[(String, Long)] = filterdDStream.map {
        case log =>
          val logSplited: Array[String] = log.split(" ")
          val timestamp = logSplited(0)
          val date = new Date(timestamp.toLong)
          val datekey = DateUtils.formatDateKey(date)

          val province = logSplited(1)
          val city = logSplited(2)
          val adid = logSplited(4).toLong

          val key = datekey + "_" + province + "_" + city + "_" + adid

          (key, 1L)
      }

      ad2OneDStream.updateStateByKey[Long] {
        //一个SEQ[Long]:Lonng类型的集合，一个Option[Long]例句历史统计的结果
        (values: Seq[Long], buffer: Option[Long]) =>
          //        如果说，之前是存在这个状态的，那么就以之前的状态作为起点，进行值的累加
          //        var count = 0L
          //        if (buffer.isDefined) {
          //          count = buffer.get
          //        }
          //可以用buffer.getOrElse(0)代替，集合的.sum方法就不多说了
          var count: Long = buffer.getOrElse(0)
          val sum: Long = values.sum
          count += sum
          Some(count)
      }
    }

    updateCountByAdDStream.foreachRDD(
      rdd =>
        rdd.foreachPartition {
          items =>
            val statArray = new ArrayBuffer[AdStat]()
            for (item <- items) {
              val keySplited: Array[String] = item._1.split("_")
              val date = keySplited(0)
              val province = keySplited(1)
              val city = keySplited(2)
              val adid = keySplited(3).toLong

              val clickCount = item._2
              statArray += AdStat(date, province, city, adid, clickCount)
            }
            AdStatDAO.updateBatch(statArray.toArray)
        }
    )

    updateCountByAdDStream
  }

  /**
    * 我们要算的其实是某一天某个用户对某个广告的点击次数
    * 所以其实就是(date_user_ad,count)
    */
  def generateBlackList(filterdDStream: DStream[String]): Unit = {

    //filterdDStream.print()
    val key2OneDStream: DStream[(String, Long)] = filterdDStream.map {
      //  log : timestamp province city userid adid
      case log =>
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        // yy-mm-dd
        val dateKey = DateUtils.formatDateKey(new Date(timeStamp))
        val userId = logSplit(3).toLong
        val adid = logSplit(4).toLong

        val key = dateKey + "_" + userId + "_" + adid

        (key, 1L)
    }
    val key2CountDStream: DStream[(String, Long)] = key2OneDStream.reduceByKey(_ + _)

    //foreachPartition 批量插入数据
    key2CountDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items => {
            val adUserClickCountArray = new ArrayBuffer[AdUserClickCount]()
            for ((key, count) <- items) {
              val keySplit = key.split("_")
              val date = keySplit(0)
              val userId = keySplit(1).toLong
              val adid = keySplit(2).toLong

              adUserClickCountArray += AdUserClickCount(date, userId, adid, count)
            }

            AdUserClickCountDAO.updateBatch(adUserClickCountArray.toArray)
          }
        }
    }

    //key2CountDStream过滤，利用key从表里取出count，判断count是否大于100
    val blackKey2CountDStream: DStream[(String, Long)] = key2CountDStream.filter {
      case (key, count) => {
        val keySplit = key.split("_")
        val date = keySplit(0)
        val userId = keySplit(1).toLong
        val adid = keySplit(2).toLong

        val clickCount = AdUserClickCountDAO.findClickCountByMultiKey(date, userId, adid)

        clickCount > 100
      }
    }

    val blackUserIdDStream: DStream[Long] = blackKey2CountDStream.map {
      case (key, count) =>
        val keySplit = key.split("_")
        val userId = keySplit(1).toLong
        userId
    }
    //一个用户队多个广告点击次数大于100，那就重复了
    val distinctBlackUserIdDStream: DStream[Long] = blackUserIdDStream.transform(rdd => rdd.distinct())

    //distinctBlackUserIdDStream.print()
    //对黑名单写入数据库
    distinctBlackUserIdDStream.foreachRDD {
      //大括号太多有点乱，有些地方我就省略了
      rdd =>
        rdd.foreachPartition {
          items =>
            val blacklistArray = new ArrayBuffer[AdBlacklist]()
            for (item <- items)
              blacklistArray += AdBlacklist(item)
            //以分区为单位输出到数据库
            AdBlacklistDAO.insertBatch(blacklistArray.toArray)
        }
    }
    //this method over
  }

}
