package comneu

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shuangmm on 2017/11/25
  */
object matchID {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("read_gz_file").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("C:\\Users\\shuangmm\\Downloads\\art.txt.gz")
    val rdd1 = sc.textFile("C:\\Users\\shuangmm\\Downloads\\art_alias.txt.gz")
    val temp1 = rdd.filter(_.split("\t").length == 2).map(x => (x.split("\t")(0),x.split("\t")(1)))
    val temp2 = rdd1.map(x => (x.split("\t")(0),x.split("\t")(1)))
    val result = temp2.join(temp1)
    val idToname = result.values.map(x =>(x._1,x._2))
    result.take(10).foreach(println)
    idToname.take(100).foreach(println)
    sc.stop()
  }

}
