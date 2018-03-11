package com.basketball

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shuangmm on 2017/12/1
  */
object Basketball {
  def Z_Socre(avg : Double, avgall : Double,stat : Double) : Double = {
     (avg - avgall)/stat
  }
  def main(args: Array[String]): Unit = {
    //spark的textFile可以直接读取gz文件
    val url = "C:\\Users\\shuangmm\\Desktop\\basketball\\leagues_NBA_2006_per_game_per_game.csv"
    val conf = new SparkConf()
    conf.setAppName("FilterAndWhere").setMaster("local")
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)
    val dataDF =sqlContext.read.format("com.databricks.spark.csv")
      .option("header","true") //这里如果在csv第一行有属性的话，没有就是"false"
      .option("inferSchema",true.toString)//这是自动推断属性列的数据类型。
      .load(url).na.drop()//文件的路径
    val data = dataDF.describe("FG%","FT%","3P11","TRB")
    val mean_std = data.select("*").where("summary = 'mean' or summary = 'stddev'").collectAsList()
    val FG_mean = mean_std.get(0).get(1)
    val FG_std = mean_std.get(1).get(1)
    val FT_mean =  mean_std.get(0).get(2)
    val FT_std = mean_std.get(1).get(2)
    val P_mean = mean_std.get(0).get(3)
    val P_std = mean_std.get(1).get(3)
    val T_mean = mean_std.get(0).get(4)
    val T_std = mean_std.get(1).get(4)
    //val f = data.select("FG%").where("summary = 'mean'").collectAsList().get(0).get(0)
    val FG_result = dataDF.select(dataDF("Player"),dataDF("FG%") ,(dataDF("FG%") - FG_mean)/FG_std as "Z_Score")
    FG_result.orderBy(FG_result("Z_Score").desc).show()
    val FT_result = dataDF.select(dataDF("Player"),dataDF("FT%") ,(dataDF("FT%") - FT_mean)/FT_std as "Z_Score")
    FT_result.orderBy(FT_result("Z_Score").desc).show()
    val P_result =  dataDF.select(dataDF("Player"),dataDF("3P11") ,(dataDF("3P11") - P_mean)/P_std as "Z_Score" )
    P_result.orderBy(P_result("Z_Score").desc).show()
    val TRb_result = dataDF.select(dataDF("Player"),dataDF("TRB") ,(dataDF("TRB") - T_mean)/T_std as "Z_Score" )
    TRb_result.orderBy(TRb_result("Z_Score").desc).show()

  }

}
