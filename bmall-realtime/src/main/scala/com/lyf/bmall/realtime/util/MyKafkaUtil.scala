package com.lyf.bmall.realtime.util

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
  * kafka0.10不需要手动维护偏移量  偏移量在kafka中
  *
  * @author shkstart
  * @date 16:07
  */
object MyKafkaUtil {

  private val properties:Properties = PropertiesUtil.load("config.properties")
  val brokerList = properties.getProperty("kafka.broker.list")

  val kafkaParams = Map(

    "bootstrap.servers" -> brokerList,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "bmall",
    //如果没有初始化偏移量或者偏移量不在任务服务器上 可以使用这个属性配置
    //latest自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "latest",
    //之前配置false 自己手动提交  在forechRDD()中 获取当前的偏移量
    //然后最后调用更新数据的偏移量
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  def getKafkaStream(topic:String,ssc:StreamingContext):InputDStream[ConsumerRecord[String,String]]={
    val dtream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams)
    )
    dtream
  }
}
