package com.atguigu.sparkmall0808.realtime.app

import com.atguigu.sparkmall0808.common.RedisUtil
import com.atguigu.sparkmall0808.realtime.AdsInfo
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object AreaCityAdsPerDayApp {


//  1、	数据整理成为 RDD[day_area_city_ads,count] => reducebykey=> updatebykey()=>
//  updateStatebykey(Seq[Long],Option[Long]=>Option(Long))
//  2 、存储
//  把redis的key中对应值更新
//  hash[key,[ day_area_city_ads,count]]

  def updateAreaCityAdsPerDay(filteredAdsClickInfoDStream: DStream[AdsInfo],sparkContext: SparkContext ): DStream[(String, Long)] ={
    //数据整理成为 RDD[day_area_city_ads,count] =
    val areaCityAdsPerDayCountDStream: DStream[(String, Long)] = filteredAdsClickInfoDStream.map { adsInfo => (adsInfo.getDayAreaCityAdsIdKey(), 1L)
    }.reduceByKey(_ + _)
    //求总值 和 redis中的 incrby 二选一   推荐用redis
    val totalCountDStream: DStream[(String, Long)] = areaCityAdsPerDayCountDStream.updateStateByKey { (countSeq: Seq[Long], totalCount: Option[Long]) =>
      val sumCount: Long = countSeq.sum
      val newTotal: Long = totalCount.getOrElse(0L) + sumCount
      Some(newTotal)
    }

    sparkContext.setCheckpointDir("./checkPoint")
    totalCountDStream.foreachRDD{rdd=>
      val totalCountArray: Array[(String, Long)] = rdd.collect()

      val jedisClient: Jedis = RedisUtil.getJedisClient
      for ((key, count) <- totalCountArray ) {
        jedisClient.hset("day:area:city:adsCount",key,count.toString )
      }
      jedisClient.close()

    }
    totalCountDStream
   }

}
