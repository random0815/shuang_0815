
package com.horizon.ss

import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}


/**
  * Created by shuangmm on 2017/12/18
  */
object Producer {
  def main(args: Array[String]): Unit = {
    val topic = "words"
    val brokers ="172.17.11.182:9092,172.17.11.183:9092,172.17.11.184:9092,172.17.11.185:9092"
    val prop=new Properties()
    prop.put("metadata.broker.list",brokers)
    prop.put("zookeeper.connect", "172.17.11.182:2181,172.17.11.183:2181,172.17.11.184:2181,172.17.11.185:2181")
    prop.put("serializer.class", "kafka.serializer.StringEncoder")

    val kafkaConfig=new ProducerConfig(prop)
    val producer=new Producer[String,String](kafkaConfig)

    val content:Array[String]=new Array[String](5)
    content(0)="apple apple banana"
    content(1)="kafka produce message"
    content(2)="hello world hello"
    content(3)="wordcount topK topK"
    content(4)="hbase spark kafka"
    while (true){
      val i=(math.random*5).toInt
      producer.send(new KeyedMessage[String,String](topic,content(i)))
      println(content(i))
      Thread.sleep(200)
    }
  }

}

