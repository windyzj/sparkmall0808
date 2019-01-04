package com.atguigu.sparkmall0808.offline.utils

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class CategoryCountAccumulator  extends  AccumulatorV2[String,mutable.HashMap[String,Long]]{
    var categoryCountMap: mutable.HashMap[String, Long]= new mutable.HashMap[String, Long]()

   //是否为空
  override def isZero: Boolean = categoryCountMap.isEmpty

  // 复制
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
       val accumulator = new CategoryCountAccumulator
       accumulator.categoryCountMap++=this.categoryCountMap
       accumulator
  }

  // 重置
  override def reset(): Unit = {
    categoryCountMap=new mutable.HashMap[String, Long]()
  }

  //累加  //给对应的key加一
  override def add(key: String): Unit = {
    categoryCountMap(key)=categoryCountMap.getOrElse(key,0L)+1L
  }

  //合并
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
       val sessionMapOther: mutable.HashMap[String, Long] = other.value

    val mergedSessionMap: mutable.HashMap[String, Long] = this.categoryCountMap.foldLeft(sessionMapOther) { case (sessionOther: mutable.HashMap[String, Long], (key, count)) =>
      sessionOther(key) = sessionMapOther.getOrElse(key, 0L) + count
      sessionOther
    }
    this.categoryCountMap=mergedSessionMap

  }

  //返回
  override def value: mutable.HashMap[String, Long] = {
    categoryCountMap
  }
}
