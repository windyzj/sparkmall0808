package com.atguigu.sparkmall0808.mock.utils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object RandomNum {

  def apply(fromNum:Int,toNum:Int): Int =  {
    fromNum+ new Random().nextInt(toNum-fromNum+1)
  }
  def multi(fromNum:Int,toNum:Int,amount:Int,delimiter:String,canRepeat:Boolean) ={
    // 实现方法  在fromNum和 toNum之间的 多个数组拼接的字符串 共amount个
    //用delimiter分割  canRepeat为false则不允许重复
    if(canRepeat){
      val listBuffer = new ListBuffer[Int]()
      for (i <- 1 to amount ) {
        listBuffer += apply(fromNum,toNum)
      }
      listBuffer.mkString(delimiter)
    } else {
    val set = new mutable.HashSet[Int]()
    while  (set.size<amount ) {
      set += apply(fromNum,toNum)
    }
      set.mkString(delimiter)
    }
  }


  def main(args: Array[String]): Unit = {
    println(multi(1, 5, 3, ",", false))
  }
}

