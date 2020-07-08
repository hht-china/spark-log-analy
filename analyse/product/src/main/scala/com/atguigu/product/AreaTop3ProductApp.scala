package com.hht.product

import java.util.UUID

import com.atguigu.product.GroupConcatDistinctUDAF
import com.hht.commons.conf.ConfigurationManager
import com.hht.commons.constant.Constants
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object AreaTop3ProductApp {

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

    sparkSession.udf.register("concat_cid_cname",
      (v1: Long, v2: String, split: String) => {
        v1 + split + v2
      })
    sparkSession.udf.register("group_connect_distinct", new GroupConcatDistinctUDAF())
    //再定义一个UDF函数,可以取出json的字段的函数
    sparkSession.udf.register("get_json_object",
      (json: String, field: String) => {
        val jSONObject: JSONObject = JSONObject.fromObject(json)
        jSONObject.getString(field)
      })

    //直接用sql join查询出action和area表在range的数据
    val cid2pidRDD: RDD[(Long, Long)] = getCityIdAndProductIdRDD(sparkSession, taskParam)

    val cid2cityinfoRDD: RDD[(Long, CityAreaInfo)] = getCityId2CityInfoRDD(sparkSession)

    val joinRDD: RDD[(Long, (Long, CityAreaInfo))] = cid2pidRDD.join(cid2cityinfoRDD)

    val fullInfoRDD: RDD[(Long, String, String, Long)] = joinRDD.map {
      case (cityid, (pid, cityinfo)) =>
        val productid = pid
        val cityName = cityinfo.city_name
        val area = cityinfo.area
        (cityid, cityName, area, productid)
    }
    import sparkSession.implicits._
    fullInfoRDD.toDF("city_id", "city_name", "area", "product_id")
      .createOrReplaceTempView("tmp_pid_city_basic")

    generateTempAreaPrdocutIdTable(sparkSession)

    generateTempAreaPrdocutInfoTable(sparkSession)

    //取topN了，还要求area的等级
    val top3DF: DataFrame = getAreaTop3Product(sparkSession)

    //  这个地方如果DF里面的字段名和mysql数据库的对不上会报错
    //    top3DF.write .format("jdbc")
    //      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
    //      .option("dbtable", "area_top3_product")
    //      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
    //      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
    //      .mode(SaveMode.Append)
    //      .save()

    //那我们一般是可以让它名字对的上也是没问题的哈，但是万一哪天我改了一下字段名，代码就挂了
    //理想来讲我是希望面向对象一点，零时表转换对象，对象去面向mysql
    top3DF.rdd.map(row =>
      AreaTop3Product(taskUUID, row.getAs[String]("area"), row.getAs[String]("area_level"),
        row.getAs[Long]("product_id"), row.getAs[String]("city_info"),
        row.getAs[Long]("count"), row.getAs[String]("product_name"),
        row.getAs[String]("product_status")))
      .toDF()
      .write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "area_top3_product")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
    sparkSession.close()
  }

  def getAreaTop3Product(sparkSession: SparkSession): DataFrame = {
    //area, product_id, click_count, city_infos, product_name, product_status，count，看起来对count排序就可以了
    //但是怎么取前3，其实就是分页-----ROW_NUMBER  OVER
    val sql1 = "SELECT product_id,area,product_name,product_status,city_info,count,ROW_NUMBER() OVER(PARTITION BY area SORT BY count desc) number " +
      "FROM tmp_area_product_info_count AS pic "
    val df: DataFrame = sparkSession.sql(sql1)
    df.show()
    df.createOrReplaceTempView("tmp_product_info_count_sort_by_area")
    //case when boolean then   end field_name
    val top3Sql = "SELECT  product_id,area,product_name,product_status,city_info,count,number," +
      "CASE " +
      "WHEN area == '华北' or area == '华东' THEN 'A' " +
      "WHEN area == '华南' or area == '华中' THEN 'B' " +
      "WHEN area == '西北' or area == '西南' THEN 'C' " +
      "ELSE 'D' " +
      "END area_level " +
      "FROM tmp_product_info_count_sort_by_area AS t " +
      "WHERE t.number <= 3"

    val top3DF: DataFrame = sparkSession.sql(top3Sql)

    top3DF
  }

  def generateTempAreaPrdocutInfoTable(sparkSession: SparkSession) = {
    //需要得到area, product_id, click_count, city_infos, product_name, product_status，count
    //count 一定要groupby，city_infos就是一个聚合函数了
    // （product_status（需要判断这个商品是自营的还是第三方的）
    //product_info表里面extend_info是一个json，product_status字段代表自营的还是第三方
    //用了if（boolean,true_value,fail_value）
    val sql = "SELECT " +
      "pc.product_id,pc.area,pi.product_name,pc.city_info,pc.count," +
      "if(get_json_object(pi.extend_info,'product_status') == 0,'SELF','Third Party') product_status " +
      "FROM tmp_area_productid_count AS pc " +
      "join product_info AS pi " +
      "ON pc.product_id = pi.product_id "

    sparkSession.sql(sql).createOrReplaceTempView("tmp_area_product_info_count")
  }


  def generateTempAreaPrdocutIdTable(sparkSession: SparkSession) = {
    //area,product_id,count(*) count对应|area|product_id|count|
    //发现少了city的信息，那groupby得到的city是多个的
    // 我们想得到它就要自定义UDAF函数
    //每行呢又是一个cityid和cityname，我们需要拼接成一个字符串，又需要定义一个UDF函数
    val sql = "SELECT " +
      " area,product_id,count(*) count,group_connect_distinct(concat_cid_cname(city_id,city_name,'=')) city_info" +
      " FROM tmp_pid_city_basic" +
      " GROUP BY area,product_id"

    val df: DataFrame = sparkSession.sql(sql)

    df.createOrReplaceTempView("tmp_area_productid_count")
  }


  def getCityId2CityInfoRDD(sparkSession: SparkSession) = {
    //我们模拟一下表的数据，懒得去创建了
    val cityAreaInfoArray = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"),
      (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"),
      (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"),
      (9L, "哈尔滨", "东北"))

    // RDD[(cityId, CityAreaInfo)]
    sparkSession.sparkContext.makeRDD(cityAreaInfoArray).map {
      case (cityId, cityName, area) =>
        (cityId, CityAreaInfo(cityId, cityName, area))
    }
  }

  def getCityIdAndProductIdRDD(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate = taskParam.get(Constants.PARAM_START_DATE)
    val endDate = taskParam.get(Constants.PARAM_END_DATE)
    val sql =
      "SELECT " +
        "city_id,click_product_id " +
        "FROM user_visit_action " +
        "WHERE click_product_id IS NOT NULL and click_product_id != -1L " +
        "AND date>='" + startDate + "' " +
        "AND date<='" + endDate + "'"
    val actionDF: DataFrame = sparkSession.sql(sql)

    import sparkSession.implicits._
    val cityid2Productid: RDD[(Long, Long)] = actionDF.as[CityClickProduct].rdd
      .map(line => (line.city_id, line.click_product_id))

    cityid2Productid
  }


}
