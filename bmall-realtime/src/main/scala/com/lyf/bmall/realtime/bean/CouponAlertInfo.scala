package com.lyf.bmall.realtime.bean

/**
  * @author shkstart
  * @date 15:35
  */
case class CouponAlertInfo(
                            mid:String,
                            uids:java.util.HashSet[String],
                            itemIds:java.util.HashSet[String],
                            events:java.util.List[String],
                            ts:Long

                          )
