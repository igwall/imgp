package app
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.classification.{
  LogisticRegression,
  LogisticRegressionModel
}
import org.apache.spark.ml.feature._

object Model {

  def createModel(df: DataFrame) = {

    // We create a column with all the ratio for each line
    val dfWithRatio = df.withColumn("ratio", createRatioColumn(df)(df.col("label")))


    val dfWithIndexed: DataFrame = indexStringColumns(dfWithRatio, List("appOrSite", "os", "network", "exchange", "interests", "media", "publisher", "size", "type", "user"))

    // Create a vector with our values  :
    val vector: VectorAssembler = new VectorAssembler()
      .setInputCols(
        Array(
          "appOrSiteIndexed",
          "bidfloor",
          "exchangeIndexed",
          "mediaIndexed",
          "networkIndexed",
          "osIndexed",
          "publisherIndexed",
          "sizeIndexed",
          "userIndexed"
        )
      )
      .setOutputCol("features")

    // WE split our DF for training
    val splitDataFrame: Array[DataFrame] =
      dfWithIndexed.randomSplit(Array(0.7, 0.3)) // Need to be confirmed ?
    var training: DataFrame = splitDataFrame(0)
    var testing: DataFrame = splitDataFrame(1)

    val logisticRegression: LogisticRegression = new LogisticRegression()
      .setWeightCol("ratio")
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

    // Print the coefficients and intercept from our logistic regression
    println(
      s"Coefficients: ${logReg.coefficients} Intercept: ${logReg.intercept}"
    )
  }

  // Check the value of ratio of true values
  val ratioValue = (df: DataFrame) => getRatio(df)

  def getRatio(df: DataFrame): Double = {
    // val labelFalse = df.filter(df("label") === 0.0).count
    // val totalLabel = df.count
    // ((totalLabel - labelFalse).toDouble / totalLabel)
    0.25
  }

  val createRatioColumn = (df: DataFrame) => {
    udf { label: Double =>
      if (label == 1.0) ratioValue(df)
      else 1.0 - ratioValue(df)
    }
  }


  def indexStringColumns(df: DataFrame, cols: List[String]): DataFrame = {
    var newdf = df

    cols.foreach { col =>
      val si = new StringIndexer().setInputCol(col).setOutputCol(col + "Indexed").setHandleInvalid("keep")

      val sm: StringIndexerModel = si.fit(newdf)
      val indexed = sm.transform(newdf).drop(col)
      newdf = indexed
    }
    newdf
  }

}
