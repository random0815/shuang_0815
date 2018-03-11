package comneu

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shuangmm on 2018/1/25
  */
object FliterData {

  case class data(userId:Int,artId:Int,num:Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[4]").appName("RDD2DF").getOrCreate()
    val path = "hdfs://172.17.11.182:9000/Spark-Data"

    val atrId = spark.sparkContext.textFile("C:\\Users\\shuangmm\\Desktop\\实训\\数据集\\artist_alias.txt")
    val nameToId = spark.sparkContext.textFile("C:\\Users\\shuangmm\\Desktop\\实训\\数据集\\artist_data.txt")
    val user_artist_data = spark.sparkContext.textFile("C:\\Users\\shuangmm\\Desktop\\实训\\数据集\\user_artist_data.txt")
    val nameId = nameToId.filter(_.split("\t").length == 2).map(x => (x.split("\t")(0),x.split("\t")(1)))
    val Id = atrId.map(x => (x.split("\t")(0),x.split("\t")(1))).cache().collectAsMap()
    //将名字和id进行映射
    val result = nameId.map{
      x =>
        val key = Id.getOrElse(x._1,x._1)

        (key,x._2)

    }//.take(10).foreach(println)
    //广播变量
    val broadcastId= spark.sparkContext.broadcast(Id)
    broadcastId.value.take(8).foreach(println)

    import spark.implicits._
    val dataDF = user_artist_data.map(_.split(" "))
      .map(parts => data(parts(0).trim.toInt,parts(1).trim.toInt,parts(2).trim.toInt))
      .toDF()

    dataDF.write.parquet(path)

    val sqlContext = new SQLContext(spark.sparkContext)
    val tDF = sqlContext.read.format("parquet").load(path)
    tDF.show()

  }

}
