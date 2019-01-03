package com.atguigu.sparkmall0808.offline

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.sparkmall0808.common.ConfigurationUtil
import com.atguigu.sparkmall0808.common.bean.UserVisitAction
import com.atguigu.sparkmall0808.offline.utils.SessionAccumulator
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object OfflineApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("offline").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sessionAcc = new SessionAccumulator()
    sparkSession.sparkContext.register(sessionAcc)

    val config: FileBasedConfiguration = ConfigurationUtil("conditions.properties").config

    val conditionJsonString: String = config.getString("condition.params.json")

    val conditionJsonObj: JSONObject = JSON.parseObject(conditionJsonString)

    println(conditionJsonObj.getString("startDate"))
    //1 根据过滤条件 取出符合的日志RDD集合  成为RDD[UserVisitAction]
    val userActionRDD: RDD[UserVisitAction] = readUserVisitActionRDD(sparkSession,conditionJsonObj)

//    2 以sessionId为key 进行聚合   =》 RDD[sessionId,Iterable[UserVisitAction]]
     val sessionActionsRDD: RDD[(String, Iterable[UserVisitAction])] = userActionRDD.map{userAction=> (userAction.session_id,userAction) }.groupByKey()

    sessionActionsRDD.count()

//    3  把每个iterable  遍历 取最大时间和最小时间    ，取差 ，得session时长
    sessionActionsRDD.foreach{ case (sessionId,actionItr)=>
      var minTime:Long =0L
      var maxTime:Long =0L
      actionItr.foreach { userAction =>

        val actionTimeStr: String = userAction.action_time

        val actionTime: Date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(actionTimeStr)
        val actionTimeMs: Long = actionTime.getTime
        if (minTime != 0L) {
          minTime = math.min(minTime, actionTimeMs)
        } else {
          minTime = actionTimeMs
        }
        if (maxTime != 0L) {
          maxTime = math.max(maxTime, actionTimeMs)
        } else {
          maxTime = actionTimeMs
        }
      }
      val sessionStep: Int = actionItr.size
         //session时长
        var sessionVisitLength: Long = maxTime - minTime
      //    4 根据条件进行计数  利用累加器进行计数
      if(sessionVisitLength<=10000){
        sessionAcc.add("visitLength_10_le")
      }
      else if(sessionVisitLength>10000){
        sessionAcc.add("visitLength_10_gt")
      }
      if(sessionStep<=5){
        sessionAcc.add("visitStep_5_le")
      }
      else if(sessionVisitLength>5){
        sessionAcc.add("visitStep_5_gt")
      }
      sessionAcc.add("session_count")
    }

    val sessionCountMap: mutable.HashMap[String, Long] = sessionAcc.value
    val visitLength_10_le:Long = sessionCountMap.getOrElse("visitLength_10_le",0)
    val visitLength_10_gt:Long = sessionCountMap.getOrElse("visitLength_10_gt",0)
    val visitStep_5_le:Long = sessionCountMap.getOrElse("visitStep_5_le",0)
    val visitStep_5_gt:Long = sessionCountMap.getOrElse("visitStep_5_gt",0)
    val session_count:Long = sessionCountMap.getOrElse("session_count",0)
    //    5  求占比 =》 符合条件的计数 除以 总数
     val visitLength_10_le_ratio:Double = Math.round( (visitLength_10_le.toDouble/session_count*1000))/10D
     val visitLength_10_gt_ratio:Double = Math.round( (visitLength_10_gt.toDouble/session_count*1000))/10D
     val visitStep_5_le_ratio:Double = Math.round( (visitStep_5_le.toDouble/session_count*1000))/10D
     val visitStep_5_gt_ratio:Double = Math.round( (visitStep_5_gt.toDouble/session_count*1000))/10D

    println(s"visitLength_10_le_ratio = ${visitLength_10_le_ratio}")
    println(s"visitStep_5_le_ratio = ${visitStep_5_le_ratio}")

//    6  结果保存到mysql



  }


  def readUserVisitActionRDD(sparkSession: SparkSession, conditionJsonObj: JSONObject): RDD[UserVisitAction] = {
    var sql = " select v.* from user_visit_action v join user_info u on v.user_id=u.user_id where 1=1  "

    if (conditionJsonObj.getString("startDate") != null && conditionJsonObj.getString("startDate").length > 0) {
      sql += " and   date>= '" + conditionJsonObj.getString("startDate") + "'"
    }
    if (conditionJsonObj.getString("endDate") != null && conditionJsonObj.getString("endDate").length > 0) {
      sql += " and  date <='" + conditionJsonObj.getString("endDate") + "'"
    }
    if (conditionJsonObj.getString("startAge") != null && conditionJsonObj.getString("startAge").length > 0) {
      sql += " and  u.age >=" + conditionJsonObj.getString("startAge")
    }
    if (conditionJsonObj.getString("endAge") != null && conditionJsonObj.getString("endAge").length > 0) {
      sql += " and  u.age <=" + conditionJsonObj.getString("endAge")
    }
    println(sql)
    sparkSession.sql("use sparkmall0808");

    import sparkSession.implicits._
    sparkSession.sql(sql+ " limit 20").show
    sparkSession.sql(sql+ " limit 20").as[UserVisitAction].rdd

  }

}
