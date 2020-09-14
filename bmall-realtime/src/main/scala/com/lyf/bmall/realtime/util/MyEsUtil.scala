package com.lyf.bmall.realtime.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, DocumentResult, Index}

/**
  * @author shkstart
  * @date 17:49
  */
object MyEsUtil {
 /* def main(args: Array[String]): Unit = {
    val jest = getClient
    //插入学生和年龄
    val stu = new Stud("tom",12)

    val index = new Index.Builder(stu).index("bw05a").`type`("_doc")
      .id("admin123").build()
    val result: DocumentResult = jest.execute(index)
    val id: String = result.getId
    println(id)
    close(jest)
  }

  case class Stud(name:String,age:Int)*/

  private val ES_HOST = "http://bw77"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = null
  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 关闭客户端
    */
  def close(client: JestClient): Unit = {
    if (client != null) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)

  }

  //批量将数据插入到es
  def insertBulk(indexName:String,docList:List[(String,Any)]):Unit={
    if (docList.size>0){
      val jest: JestClient = getClient
      //构建批量的builder 设置好index type
      val bulkBuilder: Bulk.Builder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")
      //对bulkBuilder设置id build  设置值
      for((id,doc)<-docList){
        //创建indexBuilder对象  设置资源数据
        val indexBuilder = new Index.Builder(doc)
        if(id!=null){
          //设置id
          indexBuilder.id(id)
        }

        //build 构建indexBuilder
        val index: Index = indexBuilder.build()
        bulkBuilder.addAction(index)

      }
      val bulk = bulkBuilder.build()
      jest.execute(bulk)
      close(jest)
    }
  }

}
