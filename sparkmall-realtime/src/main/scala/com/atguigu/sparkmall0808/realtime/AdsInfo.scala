package com.atguigu.sparkmall0808.realtime

import java.text.SimpleDateFormat
import java.util.Date

case class AdsInfo(ts:Long ,area:String ,city:String ,userId:String ,adsId:String ) {

  def getDayAreaCityAdsIdKey(): String ={
    val dayString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
    dayString+":"+area+":"+city+":"+adsId
  }

}
