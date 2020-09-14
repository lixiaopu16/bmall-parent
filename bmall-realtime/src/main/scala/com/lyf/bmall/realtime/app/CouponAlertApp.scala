package com.lyf.bmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.lyf.bmall.common.constant.BmallConstants
import com.lyf.bmall.realtime.bean.{CouponAlertInfo, EventInfo}
import com.lyf.bmall.realtime.util.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.util.control.Breaks

/**
  * 预警数据的处理
  *
  * @author shkstart
  * @date 15:17
  */
object CouponAlertApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf,Durations.seconds(5))

    val stream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(BmallConstants.KAFKA_EVENT,ssc)

    //如果直接使用窗口函数  会出现序列化异常  所以  要先整理数据
    val infoDstream = stream.map(record => {
      val jsonStr = record.value()
      val info: EventInfo = JSON.parseObject(jsonStr, classOf[EventInfo])
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
      val arr = sdf.format(new Date(info.ts)).split(" ")
      info.logDate = arr(0)
      info.logHour = arr(1)
      info
    })
    //每个10s统计一次最近5分钟的异常数据 窗口函数
    val windowDstream = infoDstream.window(Durations.seconds(100),Durations.seconds(10))

    //对同一个设备id进行分组
    val eventGroupByMid = windowDstream.map(info=>(info.mid,info)).groupByKey()

    //实时监控所有的用户行为  如果有异常数据 将异常数据保留下来 做下一步处理  如果不是异常数据  直接放过
    //得到异常数据后  要将异常数据拼成预警日志  写入到es中
    //开始过滤  ()
    val alertDstream: DStream[(Boolean, CouponAlertInfo)] = eventGroupByMid.map { case (mid, eventInfoItr) => {
      //登录过的uid
      val uids = new java.util.HashSet[String]()
      //领取过的商品
      val itemIds = new java.util.HashSet[String]()
      //做过哪些行为
      val eventList = new java.util.ArrayList[String]()

      var alertFlag = true

      Breaks.breakable {
        for (eventInfo: EventInfo <- eventInfoItr) {
          //其他的行为我们不管  但是用户只要一领取优惠卷 我们就记录他的uid和商品信息
          if (eventInfo.evid == "coupon") {
            uids.add(eventInfo.uid)
            itemIds.add(eventInfo.itemid)
          }
          //不管用户做什么操作  都记录他
          eventList.add(eventInfo.evid)

          //如果这个Mid对应的操作里  包含有clickItem 那么这条数据肯定就不是了
          if (eventInfo.evid == "clickItem") {
            alertFlag = false
            Breaks.break()
          }

        }
      }

      //判断该条数据是否为预警信息  组成相应的值
      (uids.size() >= 3 && alertFlag, CouponAlertInfo(mid, uids, itemIds, eventList, System.currentTimeMillis()))

    }
    }

    //将预警数据保存到es  spark目前位置  没有整合es  需要我们自己去整合
    val filterDStream: DStream[(Boolean, CouponAlertInfo)] = alertDstream.filter(_._1)
    val alterInfoWithDstream: DStream[(String, CouponAlertInfo)] = filterDStream.map {
      case (flag, alterInfo) => {
        //将数据整理成(id,alterInfo)的形式 好插
        //用到幂等性原则去重  put id mid_分钟
        val min = alterInfo.ts / 1000 / 60
        val id = alterInfo.mid + "_" + min

        (id, alterInfo)

      }
    }
    alterInfoWithDstream.foreachRDD(rdd=>{
      rdd.foreachPartition(itr=>{
        MyEsUtil.insertBulk(BmallConstants.ES_INDEX_COUPONINFO,itr.toList)
      })
    })


    ssc.start()
    ssc.awaitTermination()

  }
}
