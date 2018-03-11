package pineline

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by shuangmm on 2018/1/3
  */
object PMtest {

  def main(args: Array[String]): Unit = {
    //自定义函数，将数字进行对应
    def testNum(x : Double) : Double = {
      var leveNum = 0
      if(x < 50)
        leveNum = 0
      if(x >= 50 & x <= 100)
        leveNum = 1
      if(x > 100 & x < 150)
        leveNum = 2
      if(x >= 150 & x <= 200)
        leveNum = 3
      if(x > 200 & x < 300)
        leveNum = 4
      if(x >= 300)
        leveNum = 5
      leveNum

    }
    //自定义函数，将字符串进行对应
    def testStr(x : Double) : String = {
      var leveStr = x match {
        case 0 => "优"
        case 1 => "良"
        case 2 => "轻度污染"
        case 3 => "中度污染"
        case 4 => "重度污染"
        case 5 => "严重污染"
      }
      leveStr
    }
    //注册自定义函数
    import org.apache.spark.sql.functions.{udf}
    val levelNum = udf(testNum _)
    val levelStr = udf(testStr _)
    val conf = new SparkConf().setAppName("input").setMaster("local[*]")
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)
    val url = "C:\\Users\\shuangmm\\Desktop\\pm.csv"
    val dataDF =sqlContext.read.format("csv")
      .option("header","true")
      .option("inferSchema",true.toString)//这是自动推断属性列的数据类型。
      .load(url).na.drop()
    val level = dataDF.withColumn("levelnum",levelNum(dataDF("pm")))//.show(10)
    val features = level.withColumn("levelstr",levelStr(level("levelnum")))//.show(10)
    features.show(5)
    val index = new StringIndexer()
      .setInputCol("cbwd")
      .setOutputCol("cbwd_in")

    val feat = new VectorAssembler()
      .setInputCols(Array("month","day","hour","DEWP","TEMP","PRES","cbwd_in","Iws","Is","Ir"))
      .setOutputCol("features")
    //    val data = feat.transform(dataDF)//.show(5)

    val rf = new RandomForestClassifier()
      .setLabelCol("levelnum")
      .setFeaturesCol("features")

      //.setNumTrees(10)
      .setMaxDepth(20)//20棵树  0.3189

    // Chain indexer and forest in a Pipeline
    val pipeline = new Pipeline()
      .setStages(Array(index,feat,rf))
    val Array(trainingData, testData) = features.randomSplit(Array(0.8, 0.2))

    // Train model.  This also runs the indexer.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    val result = predictions.select("levelnum","prediction", "features","levelstr")//.show(100)
    result.show(100)
    val pre = result.withColumn("predtionStr",levelStr(result("prediction")))
    pre.show(100)
    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("levelnum")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

  }

}
