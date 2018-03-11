package pineline

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{HashingTF, IDF, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by shuangmm on 2018/1/4
  */
object RandomRegressionPM {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("input").setMaster("local[*]")
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)
    val url = "C:\\Users\\shuangmm\\Desktop\\pm.csv"
    val dataDF =sqlContext.read.format("csv")
      .option("header","true")
      .option("inferSchema",true.toString)//这是自动推断属性列的数据类型。
      .load(url).na.drop()
     // dataDF.show(10)
    val index = new StringIndexer()
      .setInputCol("cbwd")
      .setOutputCol("cbwd_in")

    val feat = new VectorAssembler()
      .setInputCols(Array("month","day","hour","DEWP","TEMP","PRES","cbwd_in","Iws","Is","Ir"))
      .setOutputCol("features")
//    val data = feat.transform(dataDF)//.show(5)

    val rf = new RandomForestRegressor()
      .setLabelCol("pm")
      .setFeaturesCol("features")

    // Chain indexer and forest in a Pipeline
    val pipeline = new Pipeline()
      .setStages(Array(index,feat,rf))
    val Array(trainingData, testData) = dataDF.randomSplit(Array(0.8, 0.2))

    // Train model.  This also runs the indexer.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("pm", "prediction", "features").show(5)

    // Select (prediction, true label) and compute test error
    val evaluator = new RegressionEvaluator()
      .setLabelCol("pm")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)

//    val rfModel = model.stages(1).asInstanceOf[RandomForestRegressionModel]
 //   println("Learned regression forest model:\n" + rfModel.toDebugString)
  }

}
