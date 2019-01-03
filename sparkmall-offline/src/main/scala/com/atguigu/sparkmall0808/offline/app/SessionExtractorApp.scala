package com.atguigu.sparkmall0808.offline.app

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.sparkmall0808.common.ConfigUtil
import com.atguigu.sparkmall0808.common.bean.UserVisitAction
import com.atguigu.sparkmall0808.offline.bean.SessionInfo
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

object SessionExtractorApp {
  val needSessionNum = 1000;
   def extractSession(sessionActionsRDD: RDD[(String, Iterable[UserVisitAction])],sparkSession: SparkSession,taskId:String): Unit ={
     // 1 把 session 的动作集合 整理成sessionInfo
     val sessionInfoRDD: RDD[SessionInfo] = sessionActionsRDD.map { case (sessionId, actionItr) =>
       var minTime: Long = 0L
       var maxTime: Long = 0L

       var searchKeywordList = new ListBuffer[String]()
       var clickProductIdsList = new ListBuffer[String]()
       var orderProductIdsList = new ListBuffer[String]()
       var payProductIdsList = new ListBuffer[String]()

       actionItr.foreach { userAction =>
         //计算开始时间  结束时间
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
         // 搜索，点击 下单，支付 的商品
         if( userAction.search_keyword!=null) searchKeywordList += userAction.search_keyword
         if( userAction.click_product_id!= -1L)  clickProductIdsList += userAction.click_product_id.toString
         if( userAction.order_product_ids!=null) orderProductIdsList += userAction.order_product_ids
         if( userAction.pay_product_ids!=null)  payProductIdsList += userAction.pay_product_ids
       }
       //session步长
       val sessionStep: Int = actionItr.size
       //session时长
       var sessionVisitLength: Long = maxTime - minTime
       //开始时间
       val startTime: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(minTime))
       //行为记录，逗号分隔
       val searchKeywords: String = searchKeywordList.mkString(",")
       val clickProductIds: String = clickProductIdsList.mkString(",")
       val orderProductIds: String = orderProductIdsList.mkString(",")
       val payProductIds: String = payProductIdsList.mkString(",")
       //组合成为session
       SessionInfo(taskId, sessionId, startTime, sessionStep, sessionVisitLength, searchKeywords, clickProductIds, orderProductIds, payProductIds)
     }

     //抽数  =》 抽多少，从什么地方抽

     //      2  用天+小时作为key 进行聚合 RDD[dayhourkey,sessionInfo] =>groupbykey=> RDD[dayhourkey,iterable[sessionInfo]]
     val dayhourSessionGroupRDD: RDD[(String, Iterable[SessionInfo])] = sessionInfoRDD.map { sessionInfo =>
       val dayHour: String = sessionInfo.startTime.split(":")(0) //yyyy-MM-dd HH:mm:ss
       (dayHour, sessionInfo)
     }.groupByKey()

    //总个数  //可以使用广播变量传递
     val sessionCount: Long = sessionInfoRDD.count()
                                                                  //把list中的元素，直接压平
     val needSessionRDD: RDD[SessionInfo] = dayhourSessionGroupRDD.flatMap { case (dayhour, sessionItr) =>
       //  3  根据公式  计算出每小时要抽取session个数，
       //    每小时要抽取session个数=本小时session个数 / 总session数  * 要抽取的总session数
       val dayhourSessionCount: Int = sessionItr.size
       val dayhourNeedSessionNum: Long = Math.round(dayhourSessionCount.toDouble / sessionCount * needSessionNum)
       // 4 用这个个数从session集合中抽取相应session。
       val needSessionList: List[SessionInfo] = SessionExtractorApp.extractNum(sessionItr.toArray, dayhourNeedSessionNum.toInt)

       needSessionList
     }
     //5 保存到mysql中
     import sparkSession.implicits._

     val config: FileBasedConfiguration = ConfigUtil("config.properties").config
     needSessionRDD.toDF.write.format("jdbc")
       .option("url", config.getString("jdbc.url"))
       .option("user", config.getString("jdbc.user"))
       .option("password", config.getString("jdbc.password"))
       .option("dbtable", "random_session_info").mode(SaveMode.Append).save()


   }


  //从集合中随机抽取若干元素
  def extractNum[T] ( sourceList: Array[T],num:Int): List[T] ={

    val resultBuffer = new ListBuffer[T]()

    val indexSet = new mutable.HashSet[Int]()

    while(resultBuffer.size<num){
      // 先生成随机下标
      val index: Int = new Random().nextInt(sourceList.size)
      // 判断新产生的下标是否已经使用过
      if( ! indexSet.contains(index)){
        resultBuffer+=sourceList(index)
        indexSet+=index
      }
    }
    resultBuffer.toList
  }


  def main(args: Array[String]): Unit = {
    //println(extractNum(List(1, 2, 3, 4, 5), 3).mkString(","))

  }
}
