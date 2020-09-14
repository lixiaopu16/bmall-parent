package com.lyf.bmall.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.lyf.bmall.common.constant.BmallConstants
import com.lyf.bmall.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.lyf.bmall.realtime.util.{JedisUtil, MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * @author shkstart
  * @date 18:21
  */
object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf,Durations.seconds(5))

    //从kafka中获取order_info的数据
    val orderInfoInputDstream = MyKafkaUtil.getKafkaStream(BmallConstants.KAFKA_ORDER_INFO,ssc)
    //从kafka中获取order_detail的数据
    val orderDetailInputDstream = MyKafkaUtil.getKafkaStream(BmallConstants.KAFKA_ORDER_DETAIL,ssc)
    //从kafka中获取user的数据
    val userInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(BmallConstants.KAFKA_USER_INFO,ssc)


    val orderInfoDstream: DStream[OrderInfo] = orderInfoInputDstream.map(record => {
      val data = record.value()
      val order = JSON.parseObject(data, classOf[OrderInfo])
      val createTimeArr = order.create_time.split(" ")
      order.create_date = createTimeArr(0)
      order.create_hour = createTimeArr(1).split(":")(0)
      //对数据进行脱敏
      val phoneIp: (String, String) = order.consignee_tel.splitAt(3)
      val q3 = phoneIp._1
      val h4 = phoneIp._2.splitAt(4)._2
      order.consignee_tel = q3 + "****" + h4
      order
    })
    val orderDetailDstream: DStream[OrderDetail] = orderDetailInputDstream.map(record => {
      val data = record.value()
      val detail = JSON.parseObject(data, classOf[OrderDetail])
      detail
    })

    val userInfoDstream: DStream[UserInfo] = userInputDstream.map(record => {
      val data: String = record.value()
      val userInfo: UserInfo = JSON.parseObject(data, classOf[UserInfo])
      userInfo
    })


    //如果做join  需要将数据整理成kv的结构
    val orderInfoDstreamWithKey: DStream[(String, OrderInfo)] = orderInfoDstream.map(order=>(order.id,order))
    val orderDetailDstreamWithKey: DStream[(String, OrderDetail)] = orderDetailDstream.map(detail=>(detail.order_id,detail))

    //开始join
    /*val joinDstream: DStream[(String, (OrderInfo, OrderDetail))] = orderInfoDstreamWithKey.join(orderDetailDstreamWithKey)
    joinDstream.foreachRDD(rdd=>{
      rdd.foreach(tp=>{
        println(tp._1+" "+tp._2+"==="+tp._2._2)
      })
    })*/

    val fullouterjoinDstream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDstreamWithKey.fullOuterJoin(orderDetailDstreamWithKey)

    val saleDetailDstream: DStream[SaleDetail] = fullouterjoinDstream.mapPartitions(itr => {
      val jedis = JedisUtil.getJedisClient
      val saleDetailList = new ListBuffer[SaleDetail]()

      for ((orderId, (orderInfoOption, orderDetailOption)) <- itr) {
        if (orderInfoOption != None) {
          println("主表有数据")
          val orderInfo: OrderInfo = orderInfoOption.get
          //1.如果orderDetail也不等于none
          if (orderDetailOption != None) {
            val orderDetail: OrderDetail = orderDetailOption.get
            //2.orderInfo不等于空  orderDetail也不等于空
            //可以组成一个saleDetail
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saleDetail
          }
          //3.将当前的orderInfo对象写到redis
          //redis的key是谁 orderId   value是谁 order对象  type是谁 string
          val orderInfoKey = "order_info:" + orderId
          //JSON.toJSON(orderInfo) 不对  这个是将一个java对象转换成为一个json对象
          //如果将一个 scala的case class对象转换成json 不可以 报编译
          //阿里的json无法将case class转换成为一个json
          implicit val format = org.json4s.DefaultFormats
          val orderInfoJson: String = Serialization.write(orderInfo)
          //设置过期时间
          jedis.setex(orderInfoKey, 300, orderInfoJson)
          //从缓存中查找orderDetail
          val orderDetailKey = "order_detail:" + orderId
          val orderDetailSet: util.Set[String] = jedis.smembers(orderDetailKey)

          //一定注意 for(<-)  不能遍历java集合  需要隐式转换
          import scala.collection.JavaConversions._
          for (orderDetailJson <- orderDetailSet) {
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saleDetail
          }

        } else {
          //如果orderInfo等于空
          println("主表没数据 从表有数据")
          val orderDetail: OrderDetail = orderDetailOption.get
          //从redis中获取order_info信息
          val orderinfoKey = "order_info:" + orderId

          val orderInfoJson: String = jedis.get(orderinfoKey)

          if (orderInfoJson != null && orderInfoJson.size > 0) {

            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])

            val saleDetail = new SaleDetail(orderInfo, orderDetail)

            saleDetailList += saleDetail
          }

          println("将从表数据写入到缓存中")
          //一个orderid可能对应多个detail_id
          //redis的key是谁order_detail  value是谁  type是什么
          val orderdetailKey = "order_detail:" + orderId
          implicit val formats = org.json4s.DefaultFormats
          val orderDetailJson: String = Serialization.write(orderDetail)
          jedis.sadd(orderdetailKey, orderDetailJson)
          jedis.expire(orderdetailKey, 300)

        }
      }
      jedis.close()
      saleDetailList.toIterator
    })
    saleDetailDstream.foreachRDD(rdd=>{
      println(rdd.collect().mkString("\n"))
    })

    //将userInfo的信息  写入到redis
    userInfoDstream.foreachRDD(rdd=>{
      rdd.foreachPartition(itr=>{
        val jedis: Jedis = JedisUtil.getJedisClient
        implicit val formats = org.json4s.DefaultFormats
        itr.foreach(user=>{
          val userKey = "user_info:"+user.id
          val userjson: String = Serialization.write(user)
          jedis.set(userKey,userjson)
        })
        jedis.close()
      })
    })

    //将saleDstream和user关联
    val saleDetailWithUser: DStream[SaleDetail] = saleDetailDstream.mapPartitions(itr => {
      val jedis: Jedis = JedisUtil.getJedisClient
      val dataList = ListBuffer[SaleDetail]()

      for (detail <- itr) {
        println(detail)
        val userInfoJson = jedis.get("user_info:" + detail.user_id)
        println(userInfoJson)
        val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
        detail.mergeUserInfo(userInfo)
        dataList += detail
      }
      jedis.close()

      dataList.toIterator
    })


    //将saledetail写入到es
    saleDetailWithUser.foreachRDD(rdd=>{
      rdd.foreachPartition(itr=>{
        val saleDetailItr: Iterator[(String, SaleDetail)] = itr.map(saleDetail=>(saleDetail.order_id,saleDetail))
        MyEsUtil.insertBulk(BmallConstants.ES_INDEX_SALE,saleDetailItr.toList)
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
