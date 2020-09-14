package com.lyf.bmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lyf.bmall.common.constant.BmallConstants
import com.lyf.bmall.realtime.bean.StartUpLog
import com.lyf.bmall.realtime.util.{JedisUtil, MyKafkaUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * 数仓思想（dwd+dws） dws_uv_detail
  * 统计uv主题 搞明细数据 其实就是构建dws层
  * 1.从kafka中获取数据
  * 2.整理数据结构  将json数据转化为case class
  * 3.根据清单 使用redis进行过滤
  * 4.过滤完后的数据  还要放入redis
  * 5.将明细数据保存到hbase
  *
  * @author shkstart
  * @date 15:21
  */
object DauApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("dau")
    val ssc = new StreamingContext(conf,Durations.seconds(5))

    //spark整合phoneix的隐式转换
    import org.apache.phoenix.spark._

    //从kafka中获取数据
    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(BmallConstants.KAFKA_STARTUP,ssc)

    //实时项目中  foreachRDD中的代码是每隔5s执行一次
    inputDstream.foreachRDD(rdd=>{
      val record = rdd.map(_.value())
      //将数据从json结构转换成为case class形式
      val startUpLog: RDD[StartUpLog] = record.map(json => {

        val log: StartUpLog = JSON.parseObject(json, classOf[StartUpLog])

        //根据日期和小时去统计uv

        val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

        val dateArr: Array[String] = sdf.format(new Date(log.ts)).split(" ")

        log.logDate = dateArr(0)
        log.logHour = dateArr(1)

        log

      })

      //缓存
      startUpLog.cache()

      //将改5s中批次数据 搞成明细数据 放入到hbase
      //需要根据mid进行去重 使用redis进行去重
      //如果一条一条过滤  很慢  ing该将redis中所有数据取出来 之后广播
      val jedis = JedisUtil.getJedisClient

      val key = "dau:"+new SimpleDateFormat("yyyy-MM-dd").format(new Date())

      val midSet: util.Set[String] = jedis.smembers(key)

      jedis.close()

      println("#########截止到当前位置统计过的"+midSet.toArray().toBuffer)

      //广播到每个节点
      val bc = ssc.sparkContext.broadcast(midSet)

      //对数据进行过滤
      println("过滤之前:"+startUpLog.count())

      val filtered: RDD[StartUpLog] = startUpLog.filter(log=>{!bc.value.contains(log.mid)})

      println("过滤之后:"+filtered.count())

      //将批次内部的数据进行过滤
      val grouped: RDD[(String, Iterable[StartUpLog])] = filtered.map(log=>(log.mid,log)).groupByKey()

      val result: RDD[StartUpLog] = grouped.flatMap {
        case (mid, itr) => {
          itr.toList.take(1)
        }
      }

      //将此result放入到hbase
      result.saveToPhoenix("BMALL_DAU",Seq("AREA" ,
        "UID" ,
        "OS" ,
        "CH" ,
        "APPID" ,
        "MID" ,
        "TYPE" ,
        "VS" ,
        "TS" ,
        "LOGDATE" ,
        "LOGHOUR" ),new Configuration(),Some("bw77,bw78,bw79:2181"))


      //将统计结果 放入到reids
      result.foreachPartition(itr=>{

        val jedis = JedisUtil.getJedisClient

        itr.foreach(log=>{
          //将数据放到redis中
          //要往redis中放数据  那一定要问自己key value redis的类型是谁
          //key应该是当天的日期  value是设备id  类型是set
          val key = "dau:"+log.logDate

          jedis.sadd(key,log.mid)

        })

        jedis.close()

      })

    })

    ssc.start()
    ssc.awaitTermination()
  }
}
