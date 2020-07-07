package com.hht.session

import java.util.{Date, UUID}

import com.hht.commons.conf.ConfigurationManager
import com.hht.commons.constant.Constants
import com.hht.commons.model.{UserInfo, UserVisitAction}
import com.hht.commons.utils._
import net.sf.json.JSONObject
import org.apache.spark
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random


/**
  * 用户访问session统计分析
  *
  * 接收用户创建的分析任务，用户可能指定的条件如下：
  *
  * 1、时间范围：起始日期~结束日期
  * 2、性别：男或女
  * 3、年龄范围
  * 4、职业：多选
  * 5、城市：多选
  * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
  * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
  *
  * @author hht
  *
  */

object UserVisitSessionAnalyze {


  def main(args: Array[String]): Unit = {
    // 获取统计任务参数【为了方便，直接从配置文件中获取，企业中会从一个调度平台获取】
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    // 任务的执行ID，用户唯一标示运行后的结果，用在MySQL数据库中
    val taskUUID = UUID.randomUUID().toString

    // 构建Spark上下文
    val sparkConf = new SparkConf().setAppName("SessionAnalyzer").setMaster("local[*]")

    // 创建Spark客户端
    val sparkSession: SparkSession = new spark.sql.SparkSession.Builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext

    // 首先要从user_visit_action的Hive表中，查询出来指定日期范围内的行为数据
    val userVisitActionRdd: RDD[UserVisitAction] = getActionRDDByRange(sparkSession, taskParam)

    // 将用户行为信息转换为 K-V 结构
    val session2UserVisitActionRDD: RDD[(String, UserVisitAction)] = userVisitActionRdd.map(userVisitAction => (userVisitAction.session_id, userVisitAction))

    // 将数据进行内存缓存
    session2UserVisitActionRDD.cache() // session2UserVisitActionRDD.persist(StorageLevel.MEMORY_ONLY)

    val sessionId2FullAggrInfoRDD: RDD[(String, String)] = aggrerateBySession(sparkSession, session2UserVisitActionRDD)

    /**
      * 过滤无用的数据
      * 统计时长范围和步长范围 和之前demo的count不同的，他是区间，比如（1_3S，20）
      * 全局累加肯定是累加器
      * 那1_3S这种你要怎么去累加，自定义累加器（MAP(区间，次数)）
      *
      * 过滤要对单条数据过滤，累加也要单条数据循环，好像可以放在一起，
      * 如果数据无用直接不操作，符合条件才操作并返回success
      */

    val stepAndVisitLengthAccumulator = new StepAndVisitLengthAccumulator()
    sc.register(stepAndVisitLengthAccumulator, "stepAndVisitLengthAccumulator")
    //过滤掉无用的数据，同时对符合条件的数据进行逻辑处理（进行累计）
    val filterSessionId2FullAggrInfoRDD: RDD[(String, String)] = filterSessionAndFullAggrStat(sessionId2FullAggrInfoRDD, taskParam, stepAndVisitLengthAccumulator)

    //用完累加器要缓存的
    filterSessionId2FullAggrInfoRDD.cache()

    //触发action算子
    filterSessionId2FullAggrInfoRDD.count()

    /*
    业务功能一：统计各个范围的session占比，并写入MySQL
     */
    saveStepAndVisitLength(sparkSession, stepAndVisitLengthAccumulator.value, taskUUID)


    //=====================抽取出来的业务rdd======================
    //我现在需要一个过滤后的原始session访问明细数据(sessionId, UserVisitAction)的一个RDD
    //        我们可以直接把过滤后的rdd和原始rdd做join
    val filterdSessionId2UserVisitActionRDD: RDD[(String, UserVisitAction)] = getFilterdSessionId2UserVisitActionRDD(filterSessionId2FullAggrInfoRDD, session2UserVisitActionRDD)

    // 对数据进行内存缓存
    filterdSessionId2UserVisitActionRDD.persist(StorageLevel.MEMORY_ONLY)

    /*
       业务功能二：随机均匀获取Session
            对在每天每个小时 的 这么多session里（date，hour，（sessionList））抽取数据
            用startTime判断session所处的时间
     */
    randomExtractSession(sparkSession, taskUUID, filterSessionId2FullAggrInfoRDD, filterdSessionId2UserVisitActionRDD)


    /*   业务功能三：获取top10热门品类
            过滤后的比较原始的行为数据，对品类进行聚合
     */
    val top10CategoryList: Array[(CategorySortKey, String)] = getTop10Category(sparkSession, taskUUID, filterdSessionId2UserVisitActionRDD)


    /*
         业务功能四  Top10热门品类的每个品类的TOP10 Session记录
              topN肯定要排序的
              每个cid对应的top10 session
              那我们肯定想要一个数据格式（cid，（sessionid，count））
              然后对每一个cid的一个（sessionid，count）排序取前10
     */
    getTop10Session(sparkSession, taskUUID, top10CategoryList, filterdSessionId2UserVisitActionRDD)

    sparkSession.close()
  }

  def getTop10Session(sparkSession: SparkSession, taskUUID: String, top10CategoryList: Array[(CategorySortKey, String)], filterdSessionId2UserVisitActionRDD: RDD[(String, UserVisitAction)]): Unit = {


    // 第一步：过滤出所有点击过Top10品类的action

    //=======================方案一 使用join====================
    /*
           我这边肯定是要过滤的，但是（sessionid，UserVisitAction）和（categoryid，line）怎么join过滤
                1. （categoryid，line）肯定没啥好转化的了
                2. （sessionid，UserVisitAction）好像可以转为（categoryid，（sessionid,count））
        */
    //    先拿到所有的（category_id,category_id）
    //    val top10CategoryTuples: Array[(Long, String)] = top10CategoryList.map {
    //      case (categorySortKey, line) => {
    //        val categoryid = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_CATEGORY_ID).toLong;
    //        (categoryid, line)
    //      }
    //    }
    //    val top10CId2CountInfoRDD: RDD[(Long, String)] = sparkSession.sparkContext.makeRDD(top10CategoryTuples)
    //    val cid2ActionRDD: RDD[(Long, UserVisitAction)] = filterdSessionId2UserVisitActionRDD.map {
    //      case (seesionId, userVisitAction) => {
    //        (userVisitAction.click_category_id, userVisitAction)
    //      }
    //    }
    //    top10CId2CountInfoRDD.join(cid2ActionRDD).map{
    //      case(cid,(countInfo,action)) => {
    //        (action.session_id,action)
    //      }
    //    }


    //=======================方案二  使用filter==========
    val cidArray: Array[Long] = top10CategoryList.map {
      case (sortKey, countInfo) =>
        val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        cid
    }

    // 所有符合过滤条件的---点击过Top10热门品类的action
    val top10CategorySessionId2UserVisitActionRDD: RDD[(String, UserVisitAction)] = filterdSessionId2UserVisitActionRDD.filter {
      case (sessionid, action) => {
        // 那这个地方好像下单和支付的怎么办,因为下单和支付对这个需求没有意义
        val exit: Boolean = cidArray.contains(action.click_category_id)
        exit
      }
    }
    val top10CategorySessionId2UserVisitActionsRDD: RDD[(String, Iterable[UserVisitAction])] = top10CategorySessionId2UserVisitActionRDD.groupByKey()

    val cid2SessionCountRDD: RDD[(Long, String)] = top10CategorySessionId2UserVisitActionsRDD.flatMap {
      case (sessionId, userVisitActions) => {
        //这个操作是在sessio内部的
        val cid2Count = new mutable.HashMap[Long, Long]()
        for (userVisitAction <- userVisitActions) {
          val cid: Long = userVisitAction.click_category_id
          if (!cid2Count.contains(cid)) {
            cid2Count += (cid -> 0)
          }
          cid2Count.update(cid, cid2Count(cid) + 1)
        }
        for ((cid, count) <- cid2Count)
        //yield 会把当前的元素记下来，保存在集合中，循环结束后将返回该集合
          yield (cid, sessionId + "=" + count)
      }
    }

    //  cid2SessionCountRDD.foreach(println(_))  代码调试

    //首先有一个cid聚合是少不了的
    val cid2SessionCountListRDD: RDD[(Long, Iterable[String])] = cid2SessionCountRDD.groupByKey()

    //每个cid得到的是10个session，肯定要flatmap
    val top10SessionRDD: RDD[Top10Session] = cid2SessionCountListRDD.flatMap {
      case (cid, sessionCounts) => {
        //迭代转集合，对集合排序，取前10
        val top10Sessions: List[String] = sessionCounts.toList.sortWith(
          (item1, item2) => {
            item1.split("=")(1).toLong > item2.split("=")(1).toLong
          })
          .take(10)

        //转对象
        val sessions: List[Top10Session] = top10Sessions.map {
          case line => {
            val sessionId = line.split("=")(0)
            val count = line.split("=")(1).toLong
            Top10Session(taskUUID, cid, sessionId, count)
          }
        }
        sessions
      }
    }
    // 将结果以追加方式写入到MySQL中
    import sparkSession.implicits._
    top10SessionRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "top10_session")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

    val top10Session2SessionRDD: RDD[(String, String)] = top10SessionRDD.map(item => (item.sessionid, item.sessionid))

    // 第四步：获取top10活跃session的明细数据
    val sessionDetailRDD = top10Session2SessionRDD.join(filterdSessionId2UserVisitActionRDD).map { case (sid, (sessionid, userVisitAction)) =>
      SessionDetail(taskUUID, userVisitAction.user_id, userVisitAction.session_id,
        userVisitAction.page_id, userVisitAction.action_time, userVisitAction.search_keyword,
        userVisitAction.click_category_id, userVisitAction.click_product_id, userVisitAction.order_category_ids,
        userVisitAction.order_product_ids, userVisitAction.pay_category_ids, userVisitAction.pay_product_ids)
    }

    // 将活跃Session的明细数据，写入到MySQL
    sessionDetailRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "session_detail")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

  }

  def getClickCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {
    // 只将点击行为过滤出来
    val clickActionRDD = sessionid2detailRDD.filter { case (sessionid, userVisitAction) => userVisitAction.click_category_id != null }
    // 获取每种类别的点击次数
    // map阶段：(品类ID，1L)
    val clickCategoryIdRDD = clickActionRDD.map { case (sessionid, userVisitAction) => (userVisitAction.click_category_id, 1L) }
    // 计算各个品类的点击次数
    // reduce阶段：对map阶段的数据进行汇总
    // (品类ID1，次数) (品类ID2，次数) (品类ID3，次数) ... ... (品类ID4，次数)
    clickCategoryIdRDD.reduceByKey(_ + _)
  }

  def getOrderCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {
    // 过滤订单数据
    val orderActionRDD = sessionid2detailRDD.filter { case (sessionid, userVisitAction) => userVisitAction.order_category_ids != null }
    // 获取每种类别的下单次数
    val orderCategoryIdRDD = orderActionRDD.flatMap { case (sessionid, userVisitAction) => userVisitAction.order_category_ids.split(",").map(item => (item.toLong, 1L)) }
    // 计算各个品类的下单次数
    orderCategoryIdRDD.reduceByKey(_ + _)
  }

  def getPayCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {
    // 过滤支付数据
    val payActionRDD = sessionid2detailRDD.filter { case (sessionid, userVisitAction) => userVisitAction.pay_category_ids != null }
    // 获取每种类别的支付次数
    val payCategoryIdRDD = payActionRDD.flatMap { case (sessionid, userVisitAction) => userVisitAction.pay_category_ids.split(",").map(item => (item.toLong, 1L)) }
    // 计算各个品类的支付次数
    payCategoryIdRDD.reduceByKey(_ + _)
  }

  def joinCategoryAndData(categoryidRDD: RDD[(Long, Long)], clickCategoryId2CountRDD: RDD[(Long, Long)], orderCategoryId2CountRDD: RDD[(Long, Long)], payCategoryId2CountRDD: RDD[(Long, Long)]): RDD[(Long, String)] = {

    // 将所有品类信息与点击次数信息结合【左连接】
    val clickJoinRDD = categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD).map {
      case (categoryid, (cid, optionValue)) =>
        val clickCount = if (optionValue.isDefined) optionValue.get else 0L
        val value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|" + Constants.FIELD_CLICK_COUNT + "=" + clickCount
        (categoryid, value)
    }

    // 将所有品类信息与订单次数信息结合【左连接】
    val orderJoinRDD = clickJoinRDD.leftOuterJoin(orderCategoryId2CountRDD).map {
      case (categoryid, (ovalue, optionValue)) =>
        val orderCount = if (optionValue.isDefined) optionValue.get else 0L
        val value = ovalue + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount
        (categoryid, value)
    }

    // 将所有品类信息与付款次数信息结合【左连接】
    val payJoinRDD = orderJoinRDD.leftOuterJoin(payCategoryId2CountRDD).map {
      case (categoryid, (ovalue, optionValue)) =>
        val payCount = if (optionValue.isDefined) optionValue.get else 0L
        val value = ovalue + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount
        (categoryid, value)
    }

    payJoinRDD
  }

  /*
        业务功能三：获取top10热门品类
           过滤后的比较原始的行为数据，对品类进行聚合
           品类id有3种呢，比如点击的，下单的，支付的品类id
           那你怎么聚合

           第一步：取出所有品类id，去重

           第二步：计算各品类的点击、下单和支付的次数
              分步求出对3种类型  聚合


     */
  def getTop10Category(sparkSession: SparkSession, taskUUID: String, filterdSessionId2UserVisitActionRDD: RDD[(String, UserVisitAction)]): Array[(CategorySortKey, String)] = {

    //第一步：获取每一个Sessionid 点击过、下单过、支付过的数量

    // 获取所有产生过点击、下单、支付中任意行为的商品类别
    val categoryidRDD: RDD[(Long, Long)] = filterdSessionId2UserVisitActionRDD.flatMap {
      case (sessionId, userVisitAction) => {
        val list: ArrayBuffer[(Long, Long)] = ArrayBuffer[(Long, Long)]()
        // 一个session中点击的商品ID
        if (userVisitAction.click_category_id != null && userVisitAction.click_category_id > 0) {
          list += ((userVisitAction.click_category_id, userVisitAction.click_category_id))
        }
        // 一个session中下单的商品ID集合
        if (userVisitAction.order_category_ids != null && userVisitAction.click_category_id > 0) {
          for (orderCategoryId <- userVisitAction.order_category_ids.split(","))
            list += ((orderCategoryId.toLong, orderCategoryId.toLong))
        }
        // 一个session中支付的商品ID集合
        if (userVisitAction.pay_category_ids != null && userVisitAction.click_category_id > 0) {
          for (payCategoryId <- userVisitAction.pay_category_ids.split(","))
            list += ((payCategoryId.toLong, payCategoryId.toLong))
        }
        list
      }
    }

    val distinctCategoryIdRDD: RDD[(Long, Long)] = categoryidRDD.distinct()

    // 第二步：计算各品类的点击、下单和支付的次数
    // 计算各个品类的点击次数
    val clickCategoryId2CountRDD: RDD[(Long, Long)] = getClickCategoryId2CountRDD(filterdSessionId2UserVisitActionRDD)
    // 计算各个品类的下单次数
    val orderCategoryId2CountRDD: RDD[(Long, Long)] = getOrderCategoryId2CountRDD(filterdSessionId2UserVisitActionRDD)
    // 计算各个品类的支付次数
    val payCategoryId2CountRDD: RDD[(Long, Long)] = getPayCategoryId2CountRDD(filterdSessionId2UserVisitActionRDD)

    //到这一步我们已经得出了点击、下单和支付三中类型的  品类的聚合统计

    // 第三步：join各品类与它的点击、下单和支付的次数
    // (cid,cid) leftjoin (click_cid,count) => （cid,cid=?|clickcount=?）
    //   leftjoin(order_cid,count) =>（cid,cid=?|clickcount=?|ordercount=?） ...以此类推
    // 其实相对来讲我更喜欢用对象的方式而不是拼接字符串的方式来操作
    val categoryid2countRDD: RDD[(Long, String)] = joinCategoryAndData(distinctCategoryIdRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD, payCategoryId2CountRDD)

    //第四步  数据都搞定了，自然要排序了
    val sortKey2countRDD: RDD[(CategorySortKey, String)] = categoryid2countRDD.map {
      case (categoryid, line) =>
        val clickCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_PAY_COUNT).toLong
        (CategorySortKey(clickCount, orderCount, payCount), line)
    }

    //默认是升序的，false改为降序
    val sortedCategoryCountRDD: RDD[(CategorySortKey, String)] = sortKey2countRDD.sortByKey(false)

    // 第六步：用take(10)取出top10热门品类，并写入MySQL
    val top10CategoryList = sortedCategoryCountRDD.take(10)
    val top10Category = top10CategoryList.map { case (categorySortKey, line) =>
      val categoryid = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_CATEGORY_ID).toLong
      val clickCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_CLICK_COUNT).toLong
      val orderCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_ORDER_COUNT).toLong
      val payCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_PAY_COUNT).toLong

      Top10Category(taskUUID, categoryid, clickCount, orderCount, payCount)
    }

    // 将Map结构转化为RDD
    val top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10Category)

    // 写入MySQL之前，将RDD转化为Dataframe
    import sparkSession.implicits._
    top10CategoryRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "top10_category")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

    top10CategoryList

  }


  /**
    * 业务需求二：随机抽取session
    */
  def randomExtractSession(sparkSession: SparkSession, taskUUID: String, filterSessionId2FullAggrInfoRDD: RDD[(String, String)], filterdSessionId2UserVisitActionRDD: RDD[(String, UserVisitAction)]): Unit = {

    // 第一步，计算出每天每小时的session数量，获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD

    val time2FullAggrInfoRDD: RDD[(String, String)] = filterSessionId2FullAggrInfoRDD.map {
      case (sessionId: String, fullAggrInfo: String) => {
        //取出start_time字段
        val startTime = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_START_TIME)
        // 将key改为yyyy-MM-dd_HH的形式（小时粒度）
        val time = DateUtils.getDateHour(startTime)
        (time, fullAggrInfo)
      }
    }
    // 得到每天每小时的session数量
    // countByKey()计算每个不同的key有多少个数据
    // countMap<yyyy-MM-dd_HH, count>
    val countMap: collection.Map[String, Long] = time2FullAggrInfoRDD.countByKey()

    // 第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引，将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式

    // countMap<yyyy-MM-dd_HH, count>转成dateHourCountMap <yyyy-MM-dd,<HH,count>>
    val dateHourCountMap = mutable.HashMap[String, mutable.HashMap[String, Long]]()

    for ((time, count) <- countMap) {
      val date = time.split("_")(0)
      val hour = time.split("_")(1)

      // 通过模式匹配实现了if判断是否为空的功能
      dateHourCountMap.get(date) match {
        case None => {
          //没有就初始化一个map，把hour和count放进去
          //dateHourCountMap(date) = new mutable.HashMap[String, Long]()来简化了先创建在赋值的步骤
          dateHourCountMap(date) = new mutable.HashMap[String, Long]()
          dateHourCountMap(date) += (hour -> count)
        }
        case Some(hourCountMap) => {
          //如果key里已经存在hour，那就是更新
          hourCountMap += (hour -> count)
        }
      }
    }


    // 按时间比例随机抽取算法，总共要抽取100个session，先按照天数，进行平分

    // 获取每一天要抽取的数量
    val dateExtractCount = 100 / dateHourCountMap.size

    // 定义一个map存放结果  dateHourExtractMap[天，[小时，index列表]]
    val dateHourExtractMap = mutable.HashMap[String, mutable.HashMap[String, mutable.ListBuffer[Int]]]()
    val random = new Random()

    /*
    遍历（date,(hour,count)）  这种操作由于map的key唯一，不会重复
        （date,(hour,(indexList))） 里面如果没有date这个key，要新建一个map放进去
         然后更新hour和对应的indexList，把这一步封装为函数
      */


    /**
      * 根据每个小时应该抽取的数量，来产生随机值
      * 遍历每个小时，填充Map<date,<hour,(3,5,20,102)>>
      *
      * @param hourExtractMap 主要用来存放生成的随机值
      * @param hourCountMap   每个小时的session总数
      * @param sessionCount   当天所有的seesion总数
      */
    def hourExtractMapFunc(hourExtractIndexMap: mutable.HashMap[String, ListBuffer[Int]], hourCountMap: mutable.HashMap[String, Long], dateCount: Long) = {
      for ((hour, hourCount) <- hourCountMap) {
        var hourExtractCount: Int = ((hourCount / dateCount) * dateExtractCount).toInt
        if (hourExtractCount > hourCount) {
          hourExtractCount = hourCount.toInt
        }

        // 仍然通过模式匹配实现有则追加，无则新建
        hourExtractIndexMap.get(hour) match {
          case None => {
            hourExtractIndexMap(hour) = new mutable.ListBuffer[Int]()
            //循环0到这个hour要抽取的个数
            for (i <- 0 to hourExtractCount) {
              // 根据hour对应的session总数量随机生成下标
              var extractIndex = random.nextInt(hourCount.toInt);
              while (hourExtractIndexMap(hour).contains(extractIndex)) {
                extractIndex = random.nextInt(hourCount.toInt);
              }
              hourExtractIndexMap(hour) += (extractIndex)
            }
          }
          case Some(indexList) => {
            for (i <- 0 to hourExtractCount) {
              var extractIndex = random.nextInt(hourCount.toInt);
              // 一旦随机生成的index已经存在，重新获取，直到获取到之前没有的index
              while (hourExtractIndexMap(hour).contains(extractIndex)) {
                extractIndex = random.nextInt(hourCount.toInt);
              }
              hourExtractIndexMap(hour) += (extractIndex)
            }
          }
        }
      }
    }


    for ((date, hourCountMap) <- dateHourCountMap) {

      // 计算出这一天的session总数   hourCountMap.values.sum很方便
      val dateCount: Long = hourCountMap.values.sum

      // dateHourExtractMap[天，[小时，小时列表]]
      dateHourExtractMap.get(date) match {
        case None => {
          dateHourExtractMap(date) = new mutable.HashMap[String, ListBuffer[Int]]()
          // 更新index函数
          hourExtractMapFunc(dateHourExtractMap(date), hourCountMap, dateCount)
        }
        case Some(hourExtractMap) => {
          hourExtractMapFunc(dateHourExtractMap(date), hourCountMap, dateCount)
        }
      }
    }

    //dateHourExtractMap(date,(hour,(indexList)))已经搞到手了
    //看好了，从第二步开始到现在可都是map的操作，没有用rdd，也就是说实在driver端的

    //先把map广播出去
    val dateHourExtractMapBroadcast: Broadcast[mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]] = sparkSession.sparkContext.broadcast(dateHourExtractMap)

    //执行groupByKey算子，得到<yyyy-MM-dd_HH,(session aggrInfo)>
    val time2FullAggrInfoListRDD: RDD[(String, Iterable[String])] = time2FullAggrInfoRDD.groupByKey()

    // 第三步：遍历每天每小时的session，然后根据随机索引进行抽取,我们用flatMap算子，遍历所有的<dateHour,(session aggrInfo)>格式的数据
    val sessionRandomExtract: RDD[SessionRandomExtract] = time2FullAggrInfoListRDD.flatMap {
      case (time, fullAggrInfoList) => {
        val date = time.split("_")(0)
        val hour = time.split("_")(1)

        //广播变量需要先用value函数获取值
        val indexList = dateHourExtractMapBroadcast.value.get(date).get(hour)

        val sessionRandomExtractArray = new ArrayBuffer[SessionRandomExtract]()
        // index是在外部进行维护
        var index = 0
        for (fullAggrInfo <- fullAggrInfoList) {
          // 如果筛选List中包含当前的index，则提取此sessionAggrInfo中的数据
          if (indexList.contains(index)) {
            val sessionid = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_SESSION_ID)
            val starttime = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_START_TIME)
            val searchKeywords = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
            val clickCategoryIds = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)
            sessionRandomExtractArray += SessionRandomExtract(taskUUID, sessionid, starttime, searchKeywords, clickCategoryIds)
          }
          // index自增
          index += 1
        }

        sessionRandomExtractArray
      }
    }
    sessionRandomExtract

    /* 将抽取后的数据保存到MySQL */

    // 引入隐式转换，准备进行RDD向Dataframe的转换
    import sparkSession.implicits._
    // 为了方便地将数据保存到MySQL数据库，将RDD数据转换为Dataframe
    sessionRandomExtract.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "session_random_extract")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

    // 提取抽取出来的数据中的sessionId
    val extractSessionidsRDD = sessionRandomExtract.map(item => (item.sessionid, item.sessionid))

    // 第四步：获取抽取出来的session的明细数据
    // 根据sessionId与详细数据进行聚合
    val extractSessionDetailRDD = extractSessionidsRDD.join(filterdSessionId2UserVisitActionRDD)

    // 对extractSessionDetailRDD中的数据进行聚合，提炼有价值的明细数据
    val sessionDetailRDD = extractSessionDetailRDD.map {
      case (sid, (sessionid, userVisitAction)) =>
        SessionDetail(taskUUID, userVisitAction.user_id, userVisitAction.session_id,
          userVisitAction.page_id, userVisitAction.action_time, userVisitAction.search_keyword,
          userVisitAction.click_category_id, userVisitAction.click_product_id, userVisitAction.order_category_ids,
          userVisitAction.order_product_ids, userVisitAction.pay_category_ids, userVisitAction.pay_product_ids)
    }

    // 将明细数据保存到MySQL中
    sessionDetailRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "session_detail")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }

  def getFilterdSessionId2UserVisitActionRDD(filterSessionId2FullAggrInfoRDD: RDD[(String, String)], session2UserVisitActionRDD: RDD[(String, UserVisitAction)]): RDD[(String, UserVisitAction)] = {
    val filterdSessionId2FullInfoAndUserVisitActionRDD: RDD[(String, (String, UserVisitAction))] = filterSessionId2FullAggrInfoRDD.join(session2UserVisitActionRDD)
    val filterdSessionId2UserVisitActionRDD: RDD[(String, UserVisitAction)] = filterdSessionId2FullInfoAndUserVisitActionRDD.map(item => (item._1, item._2._2))
    filterdSessionId2UserVisitActionRDD
  }

  def saveStepAndVisitLength(sparkSession: SparkSession, value: mutable.HashMap[String, Int], taskUUID: String): Unit = {
    // 从Accumulator统计串中获取值
    val session_count = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble

    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    // 计算各个访问时长和访问步长的范围
    //报错1 计算在这个地方出现了问题
    //    1.session_count不可以为0至少给个1，分母为0是NON
    //    2. 结果不对，发现累加的时候忘了求session_count了
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    // 将统计结果封装为Domain对象
    val sessionAggrStat = SessionAggrStat(taskUUID,
      session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    //spark的话没有这个mysql的表会自动创建一个
    import sparkSession.implicits._
    val sessionAggrStatRDD = sparkSession.sparkContext.makeRDD(Array(sessionAggrStat))
    sessionAggrStatRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "session_aggr_stat")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }

  def filterSessionAndFullAggrStat(sessionId2FullAggrInfoRDD: RDD[(String, String)], taskParam: JSONObject, stepAndVisitLengthAccumulator: StepAndVisitLengthAccumulator): RDD[(String, String)] = {
    // 获取查询任务中的配置，组合成过滤条件
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)
    var _parameter = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")
    if (_parameter.endsWith("\\|")) {
      _parameter = _parameter.substring(0, _parameter.length() - 1)
    }
    val parameter = _parameter

    val filterSessionId2FullAggrInfoRDD: RDD[(String, String)] = sessionId2FullAggrInfoRDD.filter {
      case (sessionId: String, fullAggrInfo: String) => {
        // 接着，依次按照筛选条件进行过滤
        // 按照年龄范围进行过滤（startAge、endAge）
        var success = true
        if (!ValidUtils.between(fullAggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE))
          success = false

        // 按照职业范围进行过滤（professionals）
        // 互联网,IT,软件
        // 互联网
        if (!ValidUtils.in(fullAggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS))
          success = false
        // 按照城市范围进行过滤（cities）
        // 北京,上海,广州,深圳
        // 成都
        if (!ValidUtils.in(fullAggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES))
          success = false
        // 按照性别进行过滤
        // 男/女
        // 男，女
        if (!ValidUtils.equal(fullAggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX))
          success = false
        // 按照搜索词进行过滤
        // 我们的session可能搜索了 火锅,蛋糕,烧烤
        // 我们的筛选条件可能是 火锅,串串香,iphone手机
        // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
        // 任何一个搜索词相当，即通过
        if (!ValidUtils.in(fullAggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS))
          success = false
        // 按照点击品类id进行过滤
        if (!ValidUtils.in(fullAggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS))
          success = false

        //如果符合条件才进行逻辑处理（这段逻辑就是累加器）
        if (success) {

          // 计算访问时长范围
          def calculateVisitLength(visitLength: Long) {
            if (visitLength >= 1 && visitLength <= 3) {
              stepAndVisitLengthAccumulator.add(Constants.TIME_PERIOD_1s_3s);
            } else if (visitLength >= 4 && visitLength <= 6) {
              stepAndVisitLengthAccumulator.add(Constants.TIME_PERIOD_4s_6s);
            } else if (visitLength >= 7 && visitLength <= 9) {
              stepAndVisitLengthAccumulator.add(Constants.TIME_PERIOD_7s_9s);
            } else if (visitLength >= 10 && visitLength <= 30) {
              stepAndVisitLengthAccumulator.add(Constants.TIME_PERIOD_10s_30s);
            } else if (visitLength > 30 && visitLength <= 60) {
              stepAndVisitLengthAccumulator.add(Constants.TIME_PERIOD_30s_60s);
            } else if (visitLength > 60 && visitLength <= 180) {
              stepAndVisitLengthAccumulator.add(Constants.TIME_PERIOD_1m_3m);
            } else if (visitLength > 180 && visitLength <= 600) {
              stepAndVisitLengthAccumulator.add(Constants.TIME_PERIOD_3m_10m);
            } else if (visitLength > 600 && visitLength <= 1800) {
              stepAndVisitLengthAccumulator.add(Constants.TIME_PERIOD_10m_30m);
            } else if (visitLength > 1800) {
              stepAndVisitLengthAccumulator.add(Constants.TIME_PERIOD_30m);
            }
          }

          // 计算访问步长范围
          def calculateStepLength(stepLength: Long) {
            if (stepLength >= 1 && stepLength <= 3) {
              stepAndVisitLengthAccumulator.add(Constants.STEP_PERIOD_1_3);
            } else if (stepLength >= 4 && stepLength <= 6) {
              stepAndVisitLengthAccumulator.add(Constants.STEP_PERIOD_4_6);
            } else if (stepLength >= 7 && stepLength <= 9) {
              stepAndVisitLengthAccumulator.add(Constants.STEP_PERIOD_7_9);
            } else if (stepLength >= 10 && stepLength <= 30) {
              stepAndVisitLengthAccumulator.add(Constants.STEP_PERIOD_10_30);
            } else if (stepLength > 30 && stepLength <= 60) {
              stepAndVisitLengthAccumulator.add(Constants.STEP_PERIOD_30_60);
            } else if (stepLength > 60) {
              stepAndVisitLengthAccumulator.add(Constants.STEP_PERIOD_60);
            }
          }

          //别忘了统计session的总个数，一会要求占比的
          stepAndVisitLengthAccumulator.add(Constants.SESSION_COUNT)
          // 计算出session的访问时长和访问步长的范围，并进行相应的累加
          val visitLength = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
          //封装成函数
          calculateVisitLength(visitLength)
          calculateStepLength(stepLength)
        }

        success
      }
    }
    filterSessionId2FullAggrInfoRDD
  }

  /**
    * 对Session数据进行聚合
    * 聚合action
    * join用户表
    *
    * @param sparkSession
    * @param session2UserVisitActionRDD
    * @return
    */
  def aggrerateBySession(sparkSession: SparkSession, session2UserVisitActionRDD: RDD[(String, UserVisitAction)]): RDD[(String, String)] = {

    // 对行为数据按sessionId粒度进行分组
    val session2UserVisitActionListRDD: RDD[(String, Iterable[UserVisitAction])] = session2UserVisitActionRDD.groupByKey()

    // 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来，<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
    val userId2AggrActionInfo: RDD[(Long, String)] = session2UserVisitActionListRDD.map {
      case (sessionId, userVisitActionList: Iterable[UserVisitAction]) => {

        //val  partAggrInfo =  SessionID = ** | 查询词集合 = ** |点击物品类别集合 = ** |Session访问时长 = ** |Session访问步长 = ** |Session访问时间 = **

        //这些变量都要定义在外面，一个是变量作用范围，一个是大数据循环次数很多避免创建大量变量
        var clickCategoryList = new StringBuffer("")
        var serachKetWordList = new StringBuffer("")

        var userid = -1L;

        // 计算访问时长需要在知道时间，session的起始和结束时间
        var startTime: Date = null
        var endTime: Date = null

        // session的访问步长
        var stepLength = 0

        // 以session为粒度遍历每个session所有的访问行为
        userVisitActionList.foreach(userVisitAction => {
          //每个session的userid肯定是一样的，赋值一次就OK了
          if (userid == -1L) {
            userid = userVisitAction.user_id
          }

          // 实际上这里要对数据说明一下
          // 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
          // 其实，只有搜索行为，是有searchKeyword字段的
          // 只有点击品类的行为，是有clickCategoryId字段的
          // 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的

          // 我们决定是否将搜索词或点击品类id拼接到字符串中去
          // 首先要满足：不能是null值
          // 其次，之前的字符串中还没有搜索词或者点击品类id
          val search_keyword: String = userVisitAction.search_keyword
          if (StringUtils.isNotEmpty(search_keyword)) {
            serachKetWordList.append(search_keyword + ",")
          }
          val click_category_id: Long = userVisitAction.click_category_id
          if (click_category_id != null && click_category_id > 0) {
            clickCategoryList.append(click_category_id + ",")
          }

          // 计算session开始和结束时间，w我们是不知道一个session的开始和结束时间的
          //但是一段session的记录的访问时间的最大值和最小值就是开始和结束时间
          //可以定义一个初始值，去比较赋值，after和before是Date()的比较方法
          val action_time: Date = DateUtils.parseTime(userVisitAction.action_time)
          if (startTime == null) {
            startTime = action_time
          }
          if (endTime == null) {
            endTime = action_time
          }
          if (startTime.after(action_time)) {
            startTime = action_time
          }
          if (endTime.before(action_time)) {
            endTime = action_time
          }

          // 计算session访问步长,每个动作就是一个访问步子，+1
          stepLength += 1
        })
        val searchKeywords = StringUtils.trimComma(serachKetWordList.toString)
        val clickCategoryIds = StringUtils.trimComma(clickCategoryList.toString)

        // 计算session访问时长（秒为单位）
        val visitLength = (endTime.getTime - startTime.getTime) / 1000

        // 聚合数据，使用key=value|key=value
        val partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + serachKetWordList + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryList + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)
        (userid, partAggrInfo)
      }
    }

    // 查询所有用户数据，并映射成<userid,Row>的格式
    val frame: DataFrame = sparkSession.sql("select * from user_info")
    import sparkSession.implicits._
    val userInfoRDD: RDD[UserInfo] = frame.as[UserInfo].rdd
    val userid2UserInfo: RDD[(Long, UserInfo)] = userInfoRDD.map(userInfo => (userInfo.user_id, userInfo))

    // 将session粒度聚合数据，与用户信息进行join
    val userid2FullInfoRDD: RDD[(Long, (String, UserInfo))] = userId2AggrActionInfo.join(userid2UserInfo)

    // 对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
    val sessionid2FullAggrInfoRDD: RDD[(String, String)] = userid2FullInfoRDD.map { case (uid, (partAggrInfo, userInfo)) =>
      val sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID)

      val fullAggrInfo = partAggrInfo + "|" +
        Constants.FIELD_AGE + "=" + userInfo.age + "|" +
        Constants.FIELD_PROFESSIONAL + "=" + userInfo.professional + "|" +
        Constants.FIELD_CITY + "=" + userInfo.city + "|" +
        Constants.FIELD_SEX + "=" + userInfo.sex

      (sessionid, fullAggrInfo)
    }

    sessionid2FullAggrInfoRDD
  }

  def getActionRDDByRange(sparkSession: SparkSession, taskParam: JSONObject): RDD[UserVisitAction] = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val frame: DataFrame = sparkSession.sql("select * from user_visit_action where date>='" + startDate + "' and date<='" + endDate + "'")
    import sparkSession.implicits._
    val rdd: RDD[UserVisitAction] = frame.as[UserVisitAction].rdd
    rdd
  }


}
