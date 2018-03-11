package pineline
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shuangmm on 2017/12/27
  */
object JobRank {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("input").setMaster("local[*]")
    val filter = new StopRecognition()
    filter.insertStopNatures("f","b","p","d","w","v","c","u") //过滤掉标点
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)
    val url = "C:\\Users\\shuangmm\\Desktop\\data\\jobarea=010000&industrytype=01.json"
    val dataDF =sqlContext.read.format("json")
      .option("header","true")
      .option("inferSchema",true.toString)//这是自动推断属性列的数据类型。
      .load(url)//.show(10)//文件的路径
    val pay_job = dataDF.select("slry","title","expr")
    val slry_rank = pay_job.rdd.map { x =>
      ToAnalysis.parse(x.toString()).recognition(filter).toStringWithOutNature(" ")//.toString()//
      }.filter {
      x => (x.contains("数据")&x.contains("大")&x.contains("工程师"))
    }//.foreach(println)
     val rank = slry_rank.map { x =>
       try {
         val s = x.split(" ")(1)
         val s1 = x.split(" ")(0).toDouble
         val ss = s.substring(0, s.length - 1).toDouble
         val unit = x.split(" ")(1).last
         val date = x.split(" ")(2)
         val mean = (s1 + ss) / 2 * (unit match {
           case '万' => 1
           case '千' => 0.1
         } )* (date match {
           case "月" => 12
           case "年" =>1
         })
        // val des = x.split("年").last//(3)+x.split(" ")(4)//+x.lastOption
         (mean ,"大数据工程师")
       } catch {
         case ex: NumberFormatException => {
           (0.0,"无")
         }
       }
     }.sortBy(_._1,false,1).foreach(println)


  }

}
