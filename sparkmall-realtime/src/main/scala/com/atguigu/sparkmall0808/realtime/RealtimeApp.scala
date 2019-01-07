package com.atguigu.sparkmall0808.realtime

import com.atguigu.sparkmall0808.common.MyKafkaUtil
import com.atguigu.sparkmall0808.realtime.app.{AreaCityAdsPerDayApp, BlackListApp}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object RealtimeApp {

  def main(args: Array[String]): Unit = {
      val sparkConf: SparkConf = new SparkConf().setAppName("realtime_ads").setMaster("local[*]")
      val sc = new SparkContext(sparkConf)
       val ssc = new StreamingContext(sc,Seconds(5))
    val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log",ssc)

    //把字符串整理成为对象，便于操作
    val adsClickInfoDStream: DStream[AdsInfo] = recordDstream.map { record =>
      val adsArr: Array[String] = record.value().split(" ")
      AdsInfo(adsArr(0).toLong, adsArr(1), adsArr(2), adsArr(3), adsArr(4))
    }
    //  过滤掉在黑名单中的用户日志
    val filteredAdsClickInfoDStream: DStream[AdsInfo] = BlackListApp.checkUserFromBlackList(adsClickInfoDStream,sc)
    //

    BlackListApp.checkUserToBlackList(filteredAdsClickInfoDStream)

    AreaCityAdsPerDayApp.updateAreaCityAdsPerDay(filteredAdsClickInfoDStream,sc)
    ssc.start()
    ssc.awaitTermination()
  }
}
