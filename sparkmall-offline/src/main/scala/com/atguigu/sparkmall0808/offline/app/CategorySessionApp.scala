package com.atguigu.sparkmall0808.offline.app

import com.atguigu.sparkmall0808.common.{ConfigUtil, JdbcUtil}
import com.atguigu.sparkmall0808.common.bean.UserVisitAction
import com.atguigu.sparkmall0808.offline.bean.{CategoryCountInfo, CategorySession}
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object CategorySessionApp {

  def statCategoryTop10Session(categoryTop10List:List[CategoryCountInfo],userVisitActionRDD: RDD[UserVisitAction], sparkSession: SparkSession,taskId:String): Unit ={
//    1 、过滤掉不在前十的日志
    val categoryTop10ListBC: Broadcast[List[CategoryCountInfo]] = sparkSession.sparkContext.broadcast(categoryTop10List)

    val filteredActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter { userAction =>
      var flag = false
      for (categoryCountInfo <- categoryTop10ListBC.value) {
        if (userAction.click_category_id.toString == categoryCountInfo.category_id) {
          flag = true
        }
      }
      flag
    }
    //    2、统计点击次数  计算每个品类每个session的点击次数 =>RDD[cid+sessionId，count]   => 每个session的次数
    //   分组 =》      RDD[UserVisitAction]   =>  .map => RDD[cid+session,1L]     => reducebykey(_+_)
    val categorySessionCountRDD: RDD[(String, Long)] = filteredActionRDD.map{userAction=> (userAction.click_category_id+"_"+userAction.session_id,1L)}.reduceByKey(_+_)
   // println(categorySessionCountRDD.take(10).mkString("\n"))

//    3 、  取每个品类的前十  分组排名   如果用sql : rank()over()
//    RDD[cid+sessionId，count].map =>RDD[cid,(sessionId,count)]  =>   groupbykey =>
    //分组          得到   RDD[cid,Iterable(sessionId,count)]
    val cidSessionGroupRDD: RDD[(String, Iterable[(String, Long)])] = categorySessionCountRDD.map { case (cid_sessionId, count) =>
      val cidSessionIdArray: Array[String] = cid_sessionId.split("_")
      val cid: String = cidSessionIdArray(0)
      val sessionId: String = cidSessionIdArray(1)
      (cid, (sessionId, count))
    }.groupByKey()
     //排序  //      RDD[cid,Iterable(sessionId,count)] =>.map(sortwith=>take(10))
    val sessionTop10RDD: RDD[CategorySession ]  = cidSessionGroupRDD.flatMap { case (cid, sessionItr) =>
      val top10SessionCountList: List[(String, Long)] = sessionItr.toList.sortWith { (sessionCount1, sessionCount2) =>

        //  println(s"sessionCount1 = ${sessionCount1._2}")
        //   println(s"sessionCount2 = ${sessionCount2._2}")

        //sessionCount1._2 > sessionCount2._2

       //  返回true必须明确表明前面大或后面大  其他情况必须返回false,相等情况不能返回true  jdk1.7以上要求
        if (sessionCount1._2 <= sessionCount2._2) {
          false
        }
         else {
           true
        }

      }.take(10)
     // println(top10SessionCountList.mkString("\n"))
      val sessionsTop10List: List[CategorySession] = top10SessionCountList.map { case (sessionId, count) => CategorySession(taskId, cid, sessionId, count) }
      sessionsTop10List
    }
    println(sessionTop10RDD.collect().mkString("\n"))
    val paramArray: Array[Array[Any]] = sessionTop10RDD.collect().map(categorySession=>Array(categorySession.taskId,categorySession.categoryId,categorySession.sessionId,categorySession.clickCount))
  JdbcUtil.executeBatchUpdate("insert into category_top10_session_count values(?,?,?,?)",paramArray)
//    4 、 保存到mysql
//   val config: FileBasedConfiguration = ConfigUtil("config.properties").config
//    import sparkSession.implicits._
//    sessionTop10RDD.toDF.write.format("jdbc")
//      .option("url", config.getString("jdbc.url"))
//      .option("user", config.getString("jdbc.user"))
//      .option("password", config.getString("jdbc.password"))
//      .option("dbtable", "category_top10_session_count").mode(SaveMode.Append).save()


  }

}
