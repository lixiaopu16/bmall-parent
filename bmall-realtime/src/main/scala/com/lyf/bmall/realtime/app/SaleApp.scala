package com.lyf.bmall.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.lyf.bmall.common.constant.BmallConstants
import com.lyf.bmall.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.lyf.bmall.realtime.util.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer
import org.json4s.native.Serialization
/**
  * @author shkstart
  * @date 10:27
  */
object SaleApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf,Durations.seconds(5))

    val orderRecordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(BmallConstants.KAFKA_ORDER_INFO,ssc)
    val orderDetailRecordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(BmallConstants.KAFKA_ORDER_DETAIL,ssc)
    val userRecordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(BmallConstants.KAFKA_USER_INFO,ssc)

    val orderDstream: DStream[OrderInfo] = orderRecordDstream.map { record =>
      val jsonstr = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonstr, classOf[OrderInfo])

      //补充时间字段
      val datetimeArr = orderInfo.create_time.split(" ")
      orderInfo.create_date = datetimeArr(0)
      val hourStr: String = datetimeArr(1).split(":")(0)
      orderInfo.create_hour = hourStr

      //手机号脱敏
      val tuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
      orderInfo.consignee_tel = tuple._1 + "******"

      orderInfo
    }

    val orderInfoWithKeyDstream = orderDstream.map(orderInfo=>(orderInfo.id,orderInfo))

    val orderDetailDstream: DStream[OrderDetail] = orderDetailRecordDstream.map { record =>
      val jsonstr = record.value()
      val orderDetail: OrderDetail = JSON.parseObject(jsonstr, classOf[OrderDetail])
      orderDetail
    }
    val orderDetailWithKeyDstream: DStream[(String, OrderDetail)] = orderDetailDstream.map(orderDetail=>(orderDetail.order_id,orderDetail))

    val fulljoinDstream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithKeyDstream.fullOuterJoin(orderDetailWithKeyDstream)

    val saleDetailDstream: DStream[SaleDetail] = fulljoinDstream.flatMap {
      case (orderId, (orderInfoOpt, orderDetailOpt)) =>
        //如果orderInfo 不为none
        //1 如果 从表也不为none 关联从表
        //2 把自己写入缓存
        //3 查询缓存
        val saleDetailList: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()
        val jedis = new Jedis("bw77", 6379)
        implicit val formats = org.json4s.DefaultFormats
        if (orderInfoOpt != None) {
          val orderInfo: OrderInfo = orderInfoOpt.get
          //1
          if (orderDetailOpt != None) {
            val orderDetail = orderDetailOpt.get
            //合并为宽表对象
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saleDetail
          }
          //2  type : string  key : order_info:[order_id] value order_info_json
          val orderInfoKey = "order_info:" + orderInfo.id

          //使用json4s 工具把orderInfo 解析为json
          val orderInfoJson: String = Serialization.write(orderInfo)

          jedis.setex(orderInfoKey, 3600, orderInfoJson)

          //3 订单明细如何保存到redis中
          val orderDetailKey = "order_detail:" + orderInfo.id
          val orderDetailSet: util.Set[String] = jedis.smembers(orderDetailKey)
          import scala.collection.JavaConversions._
          for (orderDetail <- orderDetailSet) {
            val orderDetail = JSON.parseObject(orderInfoJson, classOf[OrderDetail])
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saleDetail
          }

        } else if (orderDetailOpt != None) {
          //如果orderInfo 为none  从表不为none
          //1 把自己写入缓存

          val orderDetail: OrderDetail = orderDetailOpt.get
          val orderDetailJson: String = Serialization.write(orderDetail)
          val orderDetailKey = "order_detail" + orderDetail.order_id
          jedis.sadd(orderDetailKey, orderDetailJson)
          jedis.expire(orderDetailKey, 3600)


          //2 查询缓存
          val orderInfoKey = "order_info" + orderDetail.order_id
          val orderInfoJson = jedis.get(orderInfoKey)
          if (orderInfoJson != null && orderDetailJson.size > 0) {
            val orderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saleDetail
          }

        }
        jedis.close()
        saleDetailList

    }

    val fullSaleDetailDstream = saleDetailDstream.mapPartitions { saleDetailItr =>

      val jedis: Jedis = new Jedis("bw77", 6379)
      val saleDetailList = new ListBuffer[SaleDetail]
      for (saleDetail <- saleDetailItr) {
        //println("saleDetail:"+saleDetail)
        val userInfoJson = jedis.get("user_info"+saleDetail.user_id) //查询缓存

        if (userInfoJson != null) {
          val userInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
          saleDetail.mergeUserInfo(userInfo) //合并数据

        }
        saleDetailList += saleDetail
      }

      jedis.close()
      saleDetailList.toIterator

    }

    fullSaleDetailDstream.foreachRDD{rdd=>
      rdd.foreachPartition{saleDetailItr=>
        val dataList: List[(String, SaleDetail)] = saleDetailItr.map(saleDetail=>(saleDetail.order_detail_id,saleDetail)).toList
        MyEsUtil.insertBulk(BmallConstants.ES_INDEX_SALE,dataList)
      }
    }

    //user新增变化数据写入缓存
    userRecordDstream.map(record=>JSON.parseObject(record.value(),classOf[UserInfo])).foreachRDD(rdd=> {

      //用户信息表的缓存
      rdd.foreachPartition{userInfoItr=>

        val jedis: Jedis = new Jedis("bw77", 6379)
        implicit val formats = org.json4s.DefaultFormats
        for (userInfo <- userInfoItr ) {
          val userInfoJson: String = Serialization.write(userInfo)
          val userInfoKey = "user_info:"+userInfo.id
          jedis.set(userInfoKey,userInfoJson)

        }
        jedis.close()
      }
    }
    )


    ssc.start()
    ssc.awaitTermination()

  }
}
