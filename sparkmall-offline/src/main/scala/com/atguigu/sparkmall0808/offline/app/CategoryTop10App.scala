package com.atguigu.sparkmall0808.offline.app

import com.atguigu.sparkmall0808.common.JdbcUtil
import com.atguigu.sparkmall0808.common.bean.UserVisitAction
import com.atguigu.sparkmall0808.offline.bean.CategoryCountInfo
import com.atguigu.sparkmall0808.offline.utils.CategoryCountAccumulator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.{immutable, mutable}

object CategoryTop10App {
    def statCategoryTop10(userActionRDD: RDD[UserVisitAction] ,sparkSession: SparkSession,taskId:String) ={
//      1	遍历全部日志，根据品类id和操作类型，进行累加。（累加器）
//      1．1定义累加器   map结构 ，key:  cid_click,  cid_order,cid_pay      value：count
          val categoryCountAccumulator = new CategoryCountAccumulator()
          sparkSession.sparkContext.register(categoryCountAccumulator)
       //1.2  遍历rdd 注意订单、支付的业务可能涉及多个品类，要根据逗号拆分，分别累加
      userActionRDD.foreach { userAction =>
        if (userAction.click_category_id != -1L) {
          categoryCountAccumulator.add(userAction.click_category_id + "_click")
        } else if (userAction.order_category_ids != null) {
          userAction.order_category_ids.split(",").foreach(cid =>
            categoryCountAccumulator.add(cid + "_order")
          )
        } else if (userAction.pay_category_ids != null) {
          userAction.pay_category_ids.split(",").foreach(cid =>
            categoryCountAccumulator.add(cid + "_pay")
          )
        }
      }
//      2   遍历完成得到 cid对应clickcount,ordercount,paycount
      val categoryCountMap: mutable.HashMap[String, Long] = categoryCountAccumulator.value
      println(categoryCountMap.mkString("\n"))
//      3 把map中相同的cid统计结果进行聚合
      val actionCountByCidMap: Map[String, mutable.HashMap[String, Long]] = categoryCountMap.groupBy { case (cidAction, count) =>
        val cid = cidAction.split("_")(0)
        cid
      }
      //4，聚合成为List[CategoryCountInfo]
      val categoryCountInfoList: List[CategoryCountInfo] = actionCountByCidMap.map { case (cid, actionMap) =>
        CategoryCountInfo(taskId, cid, actionMap.getOrElse(cid + "_click", 0L), actionMap.getOrElse(cid + "_order", 0L), actionMap.getOrElse(cid + "_pay", 0L))
      }.toList


    //  5 根据结果进行排序
      val sortedCategoryCountInfoList: List[CategoryCountInfo] = categoryCountInfoList.sortWith((categoryCountInfo1, categoryCountInfo2) =>
        // 升序 ：前小后大 返回true  前大后小 返回false
        // 降序 ：前小后大 返回false  前大后小 返回true
        if (categoryCountInfo1.clickcount < categoryCountInfo2.clickcount) {
          false
        } else if (categoryCountInfo1.clickcount == categoryCountInfo2.clickcount) {
          if (categoryCountInfo1.ordercount < categoryCountInfo2.ordercount) {
            false
          } else {
            true
          }
        } else {
          true
        }
      )

//        6 截取前10
      val top10CategoryList: List[CategoryCountInfo] = sortedCategoryCountInfoList.take(10)
//        7  调整结构 把 list 中的CategoryCountInfo 转成Array[Any]便于数据库插入
      val top10CategoryParam: List[Array[Any]] = top10CategoryList.map { categoryInfo =>
        Array(categoryInfo.taskId, categoryInfo.category_id, categoryInfo.clickcount, categoryInfo.ordercount, categoryInfo.paycount)
      }
      //8 结果保存到数据库
      JdbcUtil.executeBatchUpdate("insert into category_top10 values (?,?,?,?,?)",top10CategoryParam)
      //9 给 需求四用
      top10CategoryList

    }
}
