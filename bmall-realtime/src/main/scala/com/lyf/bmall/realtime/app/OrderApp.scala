package com.lyf.bmall.realtime.app

import com.alibaba.fastjson.JSON
import com.lyf.bmall.common.constant.BmallConstants
import com.lyf.bmall.realtime.bean.OrderInfo
import com.lyf.bmall.realtime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * @author shkstart
  * @date 20:01
  */
object OrderApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("dau")
    val ssc = new StreamingContext(conf,Durations.seconds(5))
    //spark整合phoneix的隐式转换
    import org.apache.phoenix.spark._

    //从kafka获取数据
    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(BmallConstants.KAFKA_ORDER_INFO,ssc)

    //补充时间戳
    //敏感字脱敏
    val orderDstream: DStream[OrderInfo] = inputDstream.map { record =>
      val jsonstr = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonstr, classOf[OrderInfo])

      val datetimeArr = orderInfo.create_time.split(" ")
      orderInfo.create_date = datetimeArr(0)
      val hourStr: String = datetimeArr(1).split(":")(0)
      orderInfo.create_hour = hourStr

      //手机号脱敏
      val tuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
      orderInfo.consignee_tel = tuple._1 + "******"

      orderInfo
    }

    //保存到hbase
    orderDstream.foreachRDD(rdd=>{
      rdd.saveToPhoenix("BMALL_ORDER_INFO",Seq(
        "ID",
        "CONSIGNEE",
        "CONSIGNEE_TEL",
        "TOTAL_AMOUNT",
        "ORDER_STATUS",
        "USER_ID",
        "PAYMENT_WAY",
        "DELIVERY_ADDRESS",
        "ORDER_COMMENT",
        "OUT_TRADE_NO",
        "TRADE_BODY",
        "CREATE_TIME",
        "OPERATE_TIME",
        "EXPIRE_TIME",
        "TRACKING_NO",
        "PARENT_ORDER_ID",
        "IMG_URL",
        "PROVINCE_ID",
        "CREATE_DATE",
        "CREATE_HOUR"
      ),new Configuration,Some("bw77,bw78,bw79:2181"))
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
