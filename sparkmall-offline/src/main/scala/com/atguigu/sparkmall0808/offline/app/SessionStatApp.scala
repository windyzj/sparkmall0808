package com.atguigu.sparkmall0808.offline.app

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.sparkmall0808.common.JdbcUtil
import com.atguigu.sparkmall0808.common.bean.UserVisitAction
import com.atguigu.sparkmall0808.offline.utils.SessionAccumulator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object SessionStatApp {

    def statSession( sessionActionsRDD:RDD[(String, Iterable[UserVisitAction])],  sparkSession:SparkSession,taskId:String , conditionJsonString:String ): Unit ={
      //注册累加器
      val sessionAcc = new SessionAccumulator()
      sparkSession.sparkContext.register(sessionAcc)
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
      //组织参数
      val result: Array[Any] = Array(taskId,conditionJsonString,session_count,visitLength_10_le_ratio,visitLength_10_gt_ratio,visitStep_5_le_ratio,visitStep_5_gt_ratio)
      JdbcUtil.executeUpdate("insert into session_stat_info values(?,?,?,?,?,?,?)" ,result)

    }
}
