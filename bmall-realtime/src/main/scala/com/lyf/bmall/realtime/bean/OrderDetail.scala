package com.lyf.bmall.realtime.bean

/**
  * @author shkstart
  * @date 14:41
  */
case class OrderDetail(
                      val id:String,
                      val order_id:String,
                      val sku_name:String,
                      val sku_id:String,
                      val order_price:String,
                      val img_url:String,
                      val sku_num:String,
                      val create_time:String
                      )
