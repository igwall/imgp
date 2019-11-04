package app
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.classification.{
  LogisticRegression,
  LogisticRegressionModel,
  BinaryLogisticRegressionSummary
}
import org.apache.spark.ml.feature._

object LogisticModel {

  def createModel(df: DataFrame) = {

    // We create a column with all the ratio for each line
    val dfWithRatio = balanceDataset(df)

    val dfWithIndexed: DataFrame = indexStringColumns(
      dfWithRatio,
      List(
        "appOrSite",
        "os",
        "network",
        "exchange",
        "interests",
        "media",
        "publisher",
        "size",
        "type",
        "user"
      )
    )

    // Create a vector with our values  :
    val vector: VectorAssembler = new VectorAssembler()
      .setInputCols(
        Array(
          "appOrSiteIndexed",
          "osIndexed",
          "networkIndexed",
          "exchangeIndexed",
          "interestsIndexed",
          "mediaIndexed",
          "publisherIndexed",
          "sizeIndexed",
          "typeIndexed",
          "userIndexed"
        )
      )
      .setOutputCol("features")

    // WE split our DF for training
    val splitDataFrame: Array[DataFrame] =
      dfWithIndexed.randomSplit(Array(0.8, 0.2)) // Need to be confirmed ?
    var training: DataFrame = splitDataFrame(0)
    var testing: DataFrame = splitDataFrame(1)

    val logisticRegression: LogisticRegression = new LogisticRegression()
      .setWeightCol("classWeightCol")
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(10)

    val stages = Array(vector, logisticRegression)

    val pipeline: Pipeline = new Pipeline().setStages(stages)

    //Here is where we train our model
    val model: PipelineModel = pipeline.fit(training)

    model.write.overwrite().save("./savedModel")

    val predictions: DataFrame = model.transform(testing)

    val logReg: LogisticRegressionModel =
      model.stages.last.asInstanceOf[LogisticRegressionModel]
    println(s"LogisticRegression: ${(logReg: LogisticRegressionModel)}")

    // ===== ANALYSE ======
    val summary = logReg.summary
    val binarySummary = summary.asInstanceOf[BinaryLogisticRegressionSummary]

    val roc = binarySummary.roc
    println(s"areaUnderROC: ${binarySummary.areaUnderROC}")

    val evaluator: BinaryClassificationEvaluator =
      new BinaryClassificationEvaluator()
        .setMetricName("areaUnderROC")
        .setRawPredictionCol("rawPrediction")
        .setLabelCol("label")

    // We evaluate and print out metrics, like our model accuracy
    val eval: Double = evaluator.evaluate(predictions)
    println("Test set areaunderROC/accuracy = " + eval)
    //println("Test set areaunderROC/accuracy = " + eval)
  }

  def balanceDataset(dataset: DataFrame): DataFrame = {

    // Re-balancing (weighting) of records to be used in the logistic loss objective function
    val numNegatives = dataset.filter(dataset("label") === 0).count
    val datasetSize = dataset.count
    val balancingRatio = (datasetSize - numNegatives).toDouble / datasetSize

    val calculateWeights = udf { d: Double =>
      if (d == 0.0) {
        1 * balancingRatio
      } else {
        (1 * (1.0 - balancingRatio))
      }
    }
    val weightedDataset =
      dataset.withColumn("classWeightCol", calculateWeights(dataset("label")))
    weightedDataset
  }

  def indexStringColumns(df: DataFrame, cols: List[String]): DataFrame = {
    var newdf = df

    cols.foreach { col =>
      val si = new StringIndexer()
        .setInputCol(col)
        .setOutputCol(col + "Indexed")
        .setHandleInvalid("keep")

      val sm: StringIndexerModel = si.fit(newdf)
      val indexed = sm.transform(newdf).drop(col)
      newdf = indexed
    }
    newdf
  }

}
