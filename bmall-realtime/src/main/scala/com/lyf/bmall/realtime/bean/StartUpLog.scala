package com.lyf.bmall.realtime.bean

/**
  * @author shkstart
  * @date 17:18
  */
case class StartUpLog(
                     val area:String,
                     val uid:String,
                     val os:String,
                     val ch:String,
                     val appid:String,
                     val mid:String,
                     val `type`:String,
                     val vs:String,
                     val ts:Long,
                     //我们数据中根本就没有当前日期字符串和每隔小时
                     //为了以后统计方便  加两个字段
                     var logDate:String,
                     var logHour:String
                     )
