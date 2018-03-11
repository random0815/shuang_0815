package pineline

import java.io.File

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

import scala.io.Source

/**
  * Created by shuangmm on 2017/12/31
  */
object Requirment {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("input").setMaster("local[*]")
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)
    val url = "C:\\Users\\shuangmm\\Desktop\\data\\jobarea=010000&industrytype=01.json"
    val dataDF =sqlContext.read.format("json")
      .option("header","true")
      .option("inferSchema",true.toString)//这是自动推断属性列的数据类型。
      .load(url)//文件的路径
    val strStop = Source.fromFile(new File("C:\\Users\\shuangmm\\Desktop\\stopword.txt"))("utf-8").getLines().toArray
    val dscr = dataDF.select("cate","dscr")
    val index = new StringIndexer()
      .setInputCol("cate")
      .setOutputCol("cateIndex_String")
      .fit(dscr)
    //dscr.show(10)
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("dscr")
      .setOutputCol("words")
      //.setPattern("\\")
    val wordsData = regexTokenizer.transform(dscr)
   val remover = new StopWordsRemover()
    .setStopWords(strStop)
    .setInputCol("words")
    .setOutputCol("filter")
    val re = remover.transform(wordsData)//.show(5)
    val hashingTF =
      new HashingTF().setInputCol("filter").setOutputCol("rawFeatures").setNumFeatures(1000)
    val featurizedData = hashingTF.transform(re)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
   // val rescaledData = idfModel.transform(featurizedData)
    //rescaledData.show(5)
    val Array(trainingData, testData) = featurizedData.randomSplit(Array(0.7, 0.3))
    val rf = new RandomForestClassifier()
      .setLabelCol("cateIndex_String")
      .setFeaturesCol("features")
      .setNumTrees(15)
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(index.labels)
    val pipeline = new Pipeline()
      .setStages(Array(index,idfModel, rf, labelConverter))
   // trainingData.show(5)

    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainingData)
    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("predictedLabel",  "filter").show(100)
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("cateIndex_String")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

  }

}
