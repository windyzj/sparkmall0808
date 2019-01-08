package com.atguigu.sparkmall0808.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.sparkmall0808.common.RedisUtil
import com.atguigu.sparkmall0808.realtime.AdsInfo
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.native.JsonMethods
import org.json4s.JsonDSL._
import redis.clients.jedis.Jedis
object LastHourAdsClickApp {

  //    rdd[adsInfo]=>window
  //    rdd[ads_hourMinus ,1L]=> reducebykey=>
  //      RDD[ads_hourMInus,count]=>
  //    RDD[(adsId, (hourMInus,count)].groubykey
  //    => RDD[(adsId,Iterable[(hourMinu,count)]]=>
  //    =>RDD[adsId,Map[hourMinus,count]]
  //    =>Map[adsId,Map[hourMinus,count]]
  //    Map[adsId,hourminusCountJson ]
  //    redis :  jedis.hmset[last_hour_ads_click ,  Map[adsId,hourminusCountJson ]]
  def statLastHourAdsClick(filteredAdsClickInfoDStream: DStream[AdsInfo]): Unit ={
    // 1 利用滑动窗口取近一小时数据   rdd[adsInfo]=>window
    val lastHourAdsInfoDstream: DStream[AdsInfo] = filteredAdsClickInfoDStream.window(Minutes(60),Seconds(10))
    // 2 ,按广告进行汇总计数   rdd[ads_hourMinus ,1L]=> reducebykey=>
    val hourMinuCountPerAdsDSream: DStream[(String, Long)] = lastHourAdsInfoDstream.map { adsInfo =>
      val hourMinus: String = new SimpleDateFormat("HH:mm").format(new Date(adsInfo.ts))
      (adsInfo.adsId + "_" + hourMinus, 1L)
    }.reduceByKey(_ + _)
    // 3 把相同广告的 统计汇总     RDD[ads_hourMInus,count]=>
    //    RDD[(adsId, (hourMInus,count)].groubykey
    //    => RDD[(adsId,Iterable[(hourMinu,count)]]
    val hourMinusCountGroupbyAdsDStream: DStream[(String, Iterable[(String, Long)])] = hourMinuCountPerAdsDSream.map { case (adsHourMinusKey, count) =>
      val ads: String = adsHourMinusKey.split("_")(0)
      val hourMinus: String = adsHourMinusKey.split("_")(1)
      (ads, (hourMinus, count))
    }.groupByKey()

    //4 把小时分钟的计数结果转换成json
    //    =>RDD[adsId,Map[hourMinus,count]]
    //    =>Map[adsId,Map[hourMinus,count]]
    //    Map[adsId,hourminusCountJson ]
    val hourMinusCountJsonGroupbyAdsDStream: DStream[(String, String)] = hourMinusCountGroupbyAdsDStream.map { case (ads, hourMinusItr) =>
      val hourMinusCountJson: String = JsonMethods.compact(JsonMethods.render(hourMinusItr))
      (ads, hourMinusCountJson)
    }
    hourMinusCountJsonGroupbyAdsDStream.foreachRDD(rdd=> {

      val hourMinusCountJsonArray: Array[(String, String)] = rdd.collect()

      val jedisClient: Jedis = RedisUtil.getJedisClient
      import collection.JavaConversions._
      jedisClient.hmset("last_hour_ads_click",hourMinusCountJsonArray.toMap)
      jedisClient.close()
    }

    )





  }

}
