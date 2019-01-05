package com.atguigu.sparkmall0808.offline.utils


import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap

class CityClickCountUDAF extends UserDefinedAggregateFunction{
  // 输入的数据类型  cityname  : string
  override def inputSchema: StructType = StructType(Array(StructField("city_name",StringType)))
  // 存储类型  map[cityname,count]   总数 totalcount
  override def bufferSchema: StructType = StructType(Array(StructField("city_count",MapType(StringType,LongType)),StructField("total_count",LongType)))
  //  输出的类型   string
  override def dataType: DataType = StringType
  //  校验 相同输入是否有相同的输出 true
  override def deterministic: Boolean = true
  // 初始化  map  totalcount
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=new HashMap[String,Long]()
    buffer(1)=0L
  }
  //  分区执行更新操作  把城市名 计数到 buffer中
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val cityName: String = input.getString(0)
    val cityCountMap: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
    //更新各个城市的计数
    buffer(0)=  cityCountMap +  (cityName-> (cityCountMap.getOrElse(cityName,0L)+1L))
    //更新总数
    buffer(1)=buffer.getLong(1)+1L
  }
 //  把多个buffer 合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //合并各个城市的累计
    val cityCountMap1: Map[String, Long] = buffer1.getAs[Map[String,Long]](0)
    val cityCountMap2: Map[String, Long] = buffer2.getAs[Map[String,Long]](0)
    buffer1(0)= cityCountMap1.foldLeft(cityCountMap2) { case (cityMap2, (cityName, count)) =>
      cityMap2 + (cityName -> (cityMap2.getOrElse(cityName, 0L) + count))
    }
    //合并总值的累计
    buffer1(1)=buffer1.getLong(1)+buffer2.getLong(1)


  }
  //  返回值 把buffer中的计数 展示成字符串结果
  override def evaluate(buffer: Row): Any = {
  //1 取值
    val cityCountMap: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
    val totalCount: Long = buffer.getLong(1)
    //2   截取前两个
    val cityTop2List: List[(String, Long)] = cityCountMap.toList.sortBy(_._2)(Ordering.Long.reverse).take(2)


    // 3 每个城市的累计数换算成
    var cityRemarkList: List[CityRemark] = cityTop2List.map { case (cityName, count) =>
      val cityRatio = Math.round(count.toDouble / totalCount * 1000) / 10D
      CityRemark(cityName, cityRatio )
    }

    //3 超过两个算出其他
    var otherRatio=100D
    for (cityRemark <- cityRemarkList ) {
      otherRatio -= cityRemark.cityRatio
    }
    otherRatio= Math.round(otherRatio * 1000) / 10D
       cityRemarkList  = cityRemarkList:+ CityRemark("其他",otherRatio )
    // 4 拼接成字符串
     cityRemarkList.mkString(",")
  }

  case class CityRemark(cityName:String ,cityRatio:Double ){
    override def toString: String = {
      cityName+":"+cityRatio
    }
  }


}

object CityClickCountUDAF {
  def main(args: Array[String]): Unit = {
      val arr = Array(1,2)
      arr:+3
    println(arr.mkString(","))
  }
}