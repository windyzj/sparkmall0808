package com.atguigu.sparkmall0808.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.sparkmall0808.common.RedisUtil
import org.apache.spark.rdd
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.json4s.native.JsonMethods
import org.json4s.JsonDSL._
import redis.clients.jedis.Jedis

object AreaTop3AdsApp {


//  DStream[( areaCityAdsDayCountKey,count)]=>
//  RDD[areaAdsDayPerCityCountKey,count]=>reducebykey
//  => RDD[areaAdsDayCountKey,count]
//  =>RDD[(day,(area,(adsid,count)))]. groupbykey
//  =>RDD[day,Iterable[(area,(adsid,count))]]
//  =>.Map{ Iterable[(area,(adsid,count))  =>groupby =>Iterable(area,iterable((area,(adsid,count))))
//    => sort  =>take(3)
//    => iterable(area,adsJson) }
//  =>RDD[daykey, iterable(area,adsJson)]
//  Map[daykey,Map[area,adscountJson]]


  def statAreaTop3Ads(areaCityAdsDayTotalDstream: DStream[(String, Long)] ): Unit = {
    //  DStream[( areaCityAdsDayCountKey,count)]=>
    //整理key结构 ， 进行聚合
    //  RDD[areaAdsDayPerCityCountKey,count]=>reducebykey
    val areaAdsDayTotalDstream: DStream[(String, Long)] = areaCityAdsDayTotalDstream.map { case (areaCityAdsDay, count) =>
      val areaCityAdsDayArr: Array[String] = areaCityAdsDay.split(":")
      val daykey: String = areaCityAdsDayArr(0)
      val area: String = areaCityAdsDayArr(1)
      val ads: String = areaCityAdsDayArr(3)
      (area + ":" + ads + ":" + daykey, count) //把city去掉
    }.reduceByKey(_ + _)
    //根据日期进行聚合
    val areaAdsGroupbyDayDStream: DStream[(String, Iterable[(String, (String, Long))])] = areaAdsDayTotalDstream.map { case (areaAdsDayKey, count) =>
      val areaAdsDayArr: Array[String] = areaAdsDayKey.split(":")
      //(day,(area,(adsid,count)))
      (areaAdsDayArr(2), (areaAdsDayArr(0), (areaAdsDayArr(1), count)))
    }.groupByKey()
    //  =>RDD[day,Iterable[(area,(adsid,count))]]  =>  DStream[(daykey, Map[area, adsJson])]
    val areaAdsJsonGroupbyDayDStream: DStream[(String, Map[String, String])] = areaAdsGroupbyDayDStream.map { case (daykey, areaItr) =>
      // Map[area,Iterable[(area,(ads,count))]]

      val adsCountGroupbyAreaMap: Map[String, Iterable[(String, (String, Long))]] = areaItr.groupBy { case (area, (ads, count)) => area }
      val areaAdsJsonMap: Map[String, String] = adsCountGroupbyAreaMap.map { case (area, adsGroupByAreaItr) =>
        //调整结构把 冗余的area去掉
        val adsGroupByAreaTrimItr: Iterable[(String, Long)] = adsGroupByAreaItr.map { case (area, (ads, count)) => (ads, count) }
        //排序 // 截取  //转json
        val top3AdsCountList: List[(String, Long)] = adsGroupByAreaTrimItr.toList.sortWith(_._2 > _._2).take(3)
        //fastjson gson jackson 面向java    //json4s  面向scala
        val top3AdsCountJson: String = JsonMethods.compact(JsonMethods.render(top3AdsCountList))

        (area, top3AdsCountJson)
      }
      (daykey, areaAdsJsonMap)
    }


    //  => RDD[areaAdsDayCountKey,count]
    //  =>RDD[(day,(area,(adsid,count)))]. groupbykey
    //  =>RDD[day,Iterable[(area,(adsid,count))]]
    //  =>.Map{ Iterable[(area,(adsid,count))  =>groupby =>Iterable(area,iterable((area,(adsid,count))))
    //    => sort  =>take(3)
    //    => iterable(area,adsJson) }
    //  =>RDD[daykey, iterable(area,adsJson)]
    //  Map[daykey,Map[area,adscountJson]]

   //数据保存到redis中
    areaAdsJsonGroupbyDayDStream.foreachRDD{rdd:RDD[(String, Map[String, String])]=>
      val  areaAdsCountTop3PerDayItr: Iterable[(String, Map[String, String])] =rdd.collect()
      val jedisClient: Jedis = RedisUtil.getJedisClient
      areaAdsCountTop3PerDayItr.foreach{case (daykey,areaTop3AdsCountMap)=>
        import  collection.JavaConversions._
        jedisClient.hmset("top3_ads_per_day:"+daykey,areaTop3AdsCountMap)  //隐式转换成java map
       // jedisClient.hmset("top3_ads_per_day:"+daykey,new util.HashMap[String,String]())
      }
      jedisClient.close()
    }





  }
}
