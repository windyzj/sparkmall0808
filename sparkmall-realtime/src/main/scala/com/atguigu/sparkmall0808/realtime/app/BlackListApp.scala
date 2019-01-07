package com.atguigu.sparkmall0808.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.atguigu.sparkmall0808.common.RedisUtil
import com.atguigu.sparkmall0808.realtime.AdsInfo
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlackListApp {


//  1	先过滤黑名单 （直接查询已存储的黑名单）


//  2	检查是否符合黑名单的要求（用户每天点击超过100次）
//  把每个用户每天对每个广告的点击次数记录下来
//  redis    用hash类型存储   结构：    userId_ads_date : count
//  hincrby( k,n, f,) 给某个key的某个field的值增加n
   def checkUserToBlackList(adsClickInfoDStream: DStream[AdsInfo] ): Unit ={

       //连接1  driver
       // 注意：不能把在driver中建立的连接发送给executor中执行，会报序列化错误
      adsClickInfoDStream.foreachRDD{rdd=>
        //连接2     driver

        rdd.foreachPartition{adsItr=>
          //每个分区 单独执行  executor
          val jedis =RedisUtil.getJedisClient  //在这里建立连接可以节省连接建立次数，节省开销
          for (adsInfo <- adsItr ) {
            //  hincrby( k,f,n) 给某个key的某个field的值增加n
            val daykey: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(adsInfo.ts))
            //组合成  用户+广告+日期的key
            val countKey=adsInfo.userId+":"+adsInfo.adsId+":"+daykey
            //每次点击进行累加
            jedis.hincrBy("user:ads:dayclick",countKey,1)
            //如果单日累计值达到100，则把用户加入黑名单
            val clickcount: Long = jedis.hget("user:ads:dayclick",countKey).toLong
            if(clickcount>=100){
              jedis.sadd("blacklist",adsInfo.userId)
            }
          }
          jedis.close()

        }


      }

     }
    def checkUserFromBlackList(adsClickInfoDStream: DStream[AdsInfo],sparkContext: SparkContext): DStream[AdsInfo] ={
//      val jedis = new Jedis("hadoop1",6379)
//      val blacklist: util.Set[String] = jedis.smembers("blacklist")
//      val blacklistBC: Broadcast[util.Set[String]] = sparkContext.broadcast(blacklist)  //此处程序只会执行一次 ，数据无法动态随时间变化
      val adsClickInfoFilteredDStream: DStream[AdsInfo] = adsClickInfoDStream.transform { rdd =>
        // driver中执行，每隔一个时间周期执行一次
        val jedis = RedisUtil.getJedisClient
        val blacklist: util.Set[String] = jedis.smembers("blacklist")
        val blacklistBC: Broadcast[util.Set[String]] = sparkContext.broadcast(blacklist)
        val filterRDD: RDD[AdsInfo] = rdd.filter { adsInfo => //executor
          !blacklistBC.value.contains(adsInfo.userId)
        }
       jedis.close
       filterRDD
      }
      adsClickInfoFilteredDStream
    }



}
