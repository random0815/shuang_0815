package comneu

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shuangmm on 2017/11/25
  */
object netCount {
  def main(args: Array[String]): Unit = {
    val conf =new SparkConf().setAppName("read_gz_file").setMaster("local")
    val sc =new SparkContext(conf)

    val rdd=sc.textFile("C:\\Users\\shuangmm\\Downloads\\net.gz")
    val result = rdd.flatMap(line => line.split(",").lastOption)
    val count = result.map(x => (x,1)).reduceByKey(_+_)
    //val newfile=sc.parallelize(rdd).saveAsTextFile("C:\\Users\\shuangmm\\Downloads\\out3.txt")
    count.collect().foreach(println)

  }

}
