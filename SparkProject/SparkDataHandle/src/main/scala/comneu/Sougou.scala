package comneu

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.immutable.ListMap
import scala.collection.mutable

/**
  * Created by shuangmm on 2017/11/28
  */
object Sougou {
  def main(args: Array[String]): Unit = {
    val conf =new SparkConf().setAppName("read_gz_file").setMaster("local")
    val sc =new SparkContext(conf)
    val data = sc.textFile("SogouQ2012.mini.tar.gz")
    //val data_rdd = data.collect()
    val data_rdd = data.map(_.split("\t").last).map(x => (x,1)).reduceByKey(_+_).filter(_._2 != 1).sortBy(_._2,false)
    data_rdd.take(10).foreach(println)
  }

}
