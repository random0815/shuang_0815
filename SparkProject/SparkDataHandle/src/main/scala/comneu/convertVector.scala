package comneu

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by shuangmm on 2017/11/25
  */
//将数据转换成向量类型
object convertVector {
  def main(args: Array[String]): Unit = {
    val conf =new SparkConf().setAppName("read_gz_file").setMaster("local")
    val sc =new SparkContext(conf)

    val rdd=sc.textFile("C:\\Users\\shuangmm\\Downloads\\net.gz")
    val result = rdd.map{
      line =>

        val buffer = line.split(",").toBuffer
        buffer.remove(1,3)
        val label = buffer.remove(buffer.length-1)
        val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
        vector
    }
    result.foreach(println)
  }

}
