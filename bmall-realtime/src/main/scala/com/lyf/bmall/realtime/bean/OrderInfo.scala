package com.lyf.bmall.realtime.bean

/**
  * @author shkstart
  * @date 15:55
  */
case class OrderInfo (
                       val id:String,
                       val consignee:String,
                       var consignee_tel:String,
                       val total_amount:Double,
                       val order_status:String,
                       val user_id:String,
                       val payment_way:String,
                       val delivery_address:String,
                       val order_comment:String,
                       val out_trade_no:String,
                       val trade_body:String,
                       val create_time:String,
                       val operate_time:String,
                       val expire_time:String,
                       val tracking_no:String,
                       val parent_order_id:String,
                       val img_url:String,
                       val province_id:String,
                       var create_date:String,
                       var create_hour:String
                     )



