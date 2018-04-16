
package com.horizon.ss


import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shuangmm on 2017/12/18
  */
object KafkaWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Networkcount")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
    val topic = Set("words")
    val brokers = "172.17.11.182:9092,172.17.11.183:9092,172.17.11.184:9092,172.17.11.185:9092"
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic)
    var rank = 0; //用来记录当前数据序号
    val sqlcontext = new SQLContext(sc)
    import sqlcontext.implicits._

    val lines = kafkaStream.window(Seconds(10), Seconds(3)).flatMap(line => {
      Some(line._2.toString)
    }).transform({ rdd =>
      rdd.flatMap(_.split(" ")).toDF.withColumnRenamed("_1", "word").registerTempTable("words")
      sqlcontext.sql("select word, count(*) as total from words group by word order by count(*) desc").limit(5).map(x => {
         rank += 1
        (rank, x.getString(0), x.getLong(1))
      })
  })

    val rdd2 = lines.map(line => {
      val put = new Put(Bytes.toBytes((line._1).toString()))
      put.add(Bytes.toBytes("word"),Bytes.toBytes("word"),Bytes.toBytes(line._2))//word作为列名1
      put.add(Bytes.toBytes("word"),Bytes.toBytes("count"),Bytes.toBytes(line._3.toString))//count作为列名2
      (new ImmutableBytesWritable, put)
    }).transform(rdd=>{
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum","172.17.11.182,172.17.11.183,172.17.11.184,172.17.11.185")
      conf.set("hbase.zookeeper.property.clientPort","2181")
      val admin= new HBaseAdmin(conf)
      val jobConf = new JobConf(conf)
      jobConf.setOutputFormat(classOf[TableOutputFormat])
      jobConf.set(TableOutputFormat.OUTPUT_TABLE,"kafka_ms")
      rdd.saveAsHadoopDataset(jobConf)
      rdd
    }).map(line=> line._2.getRow()).print()
    lines.print()

    ssc.start()
    ssc.awaitTermination()

}
}

