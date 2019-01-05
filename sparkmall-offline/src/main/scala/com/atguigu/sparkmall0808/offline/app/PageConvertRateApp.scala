package com.atguigu.sparkmall0808.offline.app

import com.alibaba.fastjson.JSONObject
import com.atguigu.sparkmall0808.common.{ConfigUtil, JdbcUtil}
import com.atguigu.sparkmall0808.common.bean.UserVisitAction
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PageConvertRateApp {

  def main(args: Array[String]): Unit = {
    val arr = Array(1, 2, 3)
    println(arr.slice(1,arr.length).mkString(","))
  }

  def calcPageConvertRate(userActionRDD: RDD[UserVisitAction] ,sparkSession: SparkSession,taskId:String,conditionJsonObj: JSONObject): Unit ={
//    0  所有 规定页面跳转的次数 除以 规定页面的访问次数
//      求 规定页面跳转的次数
//      和  规定页面的访问次数
//    1  先求“规定页面的访问次数”

     //1.1得到规定的页面
    val pageFlowString: String = conditionJsonObj.getString("targetPageFlow")
    val pageFlowArr: Array[String] = pageFlowString.split(",")
   // 1.2 先筛选再聚合
   // 1.2.1   取1-6 页面 所有访问日志 ->
   val prePageFlowArr: Array[String] = pageFlowArr.slice(0,pageFlowArr.length-1)

    // 1.2.2   日志按照pageid 进行聚合   先过滤再=>转换成为kv =>再聚合统计
    // ps : countbykey是行动得到scala map , reducebykey得到是rdd 如果没有后续操作可以直接取map
    val pageVisitCountMap: collection.Map[Long, Long] = userActionRDD.filter{userAction=>prePageFlowArr.contains(userAction.page_id.toString)  }.map{userAction=>(userAction.page_id,1L)}.countByKey()

    // 3 再求“规定页面跳转的次数”
    //3.1 求规定的页面跳转  利用zip进行错位组合  1，2,3,4 zip 2,3,4,5 => (1,2)(2,3)(3,4)(4,5) => (1-2,2-3.....)
    val postPageFlowArr: Array[String] = pageFlowArr.slice(1,pageFlowArr.length)
    val tuplesArray: Array[(String, String)] = prePageFlowArr.zip(postPageFlowArr) //(1,2)(2,3)(3,4)(4,5)
    val targetPageJumpArray: Array[String] = tuplesArray.map{case (prePage,postpage)=>prePage+"-"+postpage }//(1-2,2-3....6-7.)
    val targetPageJumpArrayBC: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(targetPageJumpArray)

    // 3.2 把日志 按sessionId进行聚合  =》 RDD[sessionId,iterable[UserVisitAction]]
      val sessionActionsRDD: RDD[(String, Iterable[UserVisitAction])] = userActionRDD.map{userAction=>(userAction.session_id,userAction)}.groupByKey()

      // 3.3 把iterable按时间进行排序，求出所有的跳转页面
      val pageJumpRDD: RDD[String] = sessionActionsRDD.flatMap { case (sessionId, actionItr) =>
           //3.3.1 按访问时间排序
          val sortedActionList: List[UserVisitAction] = actionItr.toList.sortWith { (userAction1, userAction2) => userAction1.action_time < userAction2.action_time }
          // 3.3.2 转换 把访问流水 转换成 //1-2,2-4,4-6
          val pageVisitList: List[Long] = sortedActionList.map { userAction => userAction.page_id }//1,2,4,6,.....
          val tuplesPageVisitList: List[(Long, Long)] = pageVisitList.slice(0, pageVisitList.length - 1).zip(pageVisitList.slice(1, pageVisitList.length)) //(1,2),(2,4)(4,6)....
          val pageJumpList: List[String] = tuplesPageVisitList.map { case (page1, page2) => page1 + "-" + page2 } //1-2,2-4,4-6
          val filteredPageJumpList: List[String] = pageJumpList.filter { pageJump => targetPageJumpArrayBC.value.contains(pageJump) } //把不是统计目标的跳转过滤掉
          filteredPageJumpList  //每个sesssion的页面跳转    map=>flatmap
      }
    //5 聚合统计目标页面的跳转次数统计  map[1-2 , 2100]
       val pageJumpCountMap: collection.Map[String, Long] = pageJumpRDD.map(pageJump=>(pageJump,1L)).countByKey()

    // 6     页面跳转次数除以对应页面的访问次数 =》 pageJumpCountMap/pageVisitCountMap
    val pageJumpRateMap: collection.Map[String, Double] = pageJumpCountMap.map { case (pageJump, jumpCount) =>
      val visitCount: Long = pageVisitCountMap.getOrElse(pageJump.split("-")(0).toLong, 0L)
      val pageJumpRatio: Double = Math.round(jumpCount.toDouble / visitCount * 1000) / 10D
      (pageJump, pageJumpRatio)
    }
    //整理结构
    val result: Iterable[Array[Any]] = pageJumpRateMap.map{case (pageJump,pageJumpRate)=>Array(taskId,pageJump,pageJumpRate)}

    //    7 保存到数据库中
     JdbcUtil.executeBatchUpdate("insert into page_convert_rate values(?,?,?) ",result)
  }

}
