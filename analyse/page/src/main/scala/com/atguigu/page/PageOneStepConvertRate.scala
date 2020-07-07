package com.hht.page

import java.util.UUID

import com.hht.commons.conf.ConfigurationManager
import com.hht.commons.constant.Constants
import com.hht.commons.model.UserVisitAction
import com.hht.commons.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

/**
  * 页面单跳转化率模块spark作业
  *
  * 页面转化率的求解思路是通过UserAction表获取一个session的所有UserAction，根据时间顺序排序后获取全部PageId
  * 然后将PageId组合成PageFlow，即1,2,3,4,5的形式（按照时间顺序排列），之后，组合为1_2, 2_3, 3_4, ...的形式
  * 然后筛选出出现在targetFlow中的所有A_B
  *
  * 对每个A_B进行数量统计，然后统计startPage的PV，之后根据targetFlow的A_B顺序，计算每一层的转化率
  *
  */
object PageOneStepConvertRate {

  def main(args: Array[String]): Unit = {

    // 获取统计任务参数【为了方便，直接从配置文件中获取，企业中会从一个调度平台获取】
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    // 任务的执行ID，用户唯一标示运行后的结果，用在MySQL数据库中
    val taskUUID = UUID.randomUUID().toString

    // 构建Spark上下文
    val sparkConf = new SparkConf().setAppName("SessionAnalyzer").setMaster("local[*]")

    // 创建Spark客户端
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = sparkSession.sparkContext

    //取出数据
    val actionRDD: RDD[UserVisitAction] = getActionRDDByDateRange(sparkSession, taskParam)

    val sessionid2ActionRDD: RDD[(String, UserVisitAction)] = actionRDD.map(action => (action.session_id, action))

    sessionid2ActionRDD.cache()

    val sessionid2ActionIteraRDD: RDD[(String, Iterable[UserVisitAction])] = sessionid2ActionRDD.groupByKey()

    // 取出我们的页面id   pageFlowStr: "1,2,3,4,5,6,7"
    val pageFlowStr = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    // pageFlowArray: Array[Long]  [1,2,3,4,5,6,7]
    val pageFlowArray = pageFlowStr.split(",")
    // pageFlowArray.slice(0, pageFlowArray.length - 1): [1,2,3,4,5,6]
    // pageFlowArray.tail: [2,3,4,5,6,7]
    // pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail): [(1,2), (2,3) , ..]
    // 最后得到targetPageSplit: List(1_2, 2_3, 3_4, 4_5, 5_6, 6_7)
    val targetPageSplit: Array[String] = pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail).map {
      case (page1, page2) => page1 + "_" + page2
    }

    val pageSplit2One: RDD[(String, Long)] = sessionid2ActionIteraRDD.flatMap {
      case (sessionid, actions) => {
        //在每个session内部的操作
        val sortByActionTimeActions: List[UserVisitAction] = actions.toList.sortWith(
          (action1, action2) => {
            DateUtils.parseTime(action1.action_time).getTime < DateUtils.parseTime(action2.action_time).getTime
          })

        //得到session访问的有序的页面id集合
        val sortPageList: List[Long] = sortByActionTimeActions.map(action => (action.page_id))

        //得到了session的访问切片
        val pageSplitList = sortPageList.slice(0, sortPageList.length - 1).zip(sortPageList.tail).map {
          case (page1, page2) => page1 + "_" + page2
        }

        //过滤掉不在我们要统计的页面切片范围内的数据
        //我用到了targetPageSplit这个变量，他是在driver端的但是我没对他修改
        //    我只是取值而已，每个task都去取一边
        //    不过广播变量也的确是一个优化方案，不用每次都去拿
        val filterdPageSplitList: List[String] = pageSplitList.filter {
          case pageSplit => {
            targetPageSplit.contains(pageSplit)
          }
        }
        val pageSplit2One: List[(String, Long)] = filterdPageSplitList.map((_, 1L))
        pageSplit2One
      }
    }
    val pageSplit2Count: collection.Map[String, Long] = pageSplit2One.countByKey()

    //我们求出的切片的数量，第一个页面直接退出了，
    //         那就没有切片，这个1_2这个跳转率就不对
    // 所以我们要单独求出第一个页面的action的数量
    val startPage: Long = pageFlowArray(0).toLong
    val startPageCount: Long = sessionid2ActionRDD.filter {
      case (sessionid, action) => {
        action.page_id == startPage
      }
    }.count()

    getPageConvert(sparkSession, taskUUID, targetPageSplit, startPageCount, pageSplit2Count)

    sparkSession.close()
  }

  def getPageConvert(sparkSession: SparkSession, taskUUID: String, targetPageSplit: Array[String], startPageCount: Long, pageSplit2Count: collection.Map[String, Long]) = {
    val pageSplitRatio = new mutable.HashMap[String, Double]()

    var lastPageCount = startPageCount.toDouble

    for (pageSplit <- targetPageSplit) {
      val currentPageSplitCount: Double = pageSplit2Count.get(pageSplit).get.toDouble
      val ratio = currentPageSplitCount / lastPageCount
      pageSplitRatio.put(pageSplit, ratio)
      lastPageCount = currentPageSplitCount
    }

    val convertStr = pageSplitRatio.map{
      case (pageSplit, ratio) => pageSplit + "=" + ratio
    }.mkString("|")

    val pageSplit = PageSplitConvertRate(taskUUID, convertStr)

    val pageSplitRatioRDD = sparkSession.sparkContext.makeRDD(Array(pageSplit))

    import sparkSession.implicits._
    pageSplitRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "page_split_convert_rate_0308")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }

  def getActionRDDByDateRange(sparkSession: SparkSession, taskParam: JSONObject): RDD[UserVisitAction] = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    import sparkSession.implicits._
    val rdd: RDD[UserVisitAction] = sparkSession.sql("select * from user_visit_action where date>='" + startDate + "' and date<='" + endDate + "'")
      .as[UserVisitAction]
      .rdd
    rdd
  }


}
