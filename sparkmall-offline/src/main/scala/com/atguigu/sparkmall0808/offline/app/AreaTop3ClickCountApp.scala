package com.atguigu.sparkmall0808.offline.app

import com.atguigu.sparkmall0808.offline.utils.CityClickCountUDAF
import org.apache.spark.sql.SparkSession

object AreaTop3ClickCountApp {


  def statAreaTop3ClickCount(sparkSession: SparkSession): Unit ={
     sparkSession.udf.register("city_remark",new CityClickCountUDAF)
      //地区跟动作表关联 并且 按 地区和商品分组计数
     sparkSession.sql("select area,click_product_id,  count(*) cnt,city_remark(c.city_name) remark from user_visit_action v join city_info c on v.city_id=c.city_id where v.click_product_id >0  group by c.area,v.click_product_id order by area ,v.click_product_id ,cnt desc") .createOrReplaceTempView("area_product_count_view")
      // 根据计数 分组（地区，商品） 取前三名
     sparkSession.sql(" select aprk.* from ( select av.*,  rank()over(partition by area order by cnt desc ) rk from area_product_count_view av )aprk where  aprk.rk<=3 "). createOrReplaceTempView("area_product_top3_view")
    //  关联商品名称
     sparkSession.sql(" select t3.area,p.product_name,t3.cnt,rk,   remark from area_product_top3_view t3 join product_info p on t3.click_product_id=p.product_id order by area ,product_name,cnt desc").write
  }
}
