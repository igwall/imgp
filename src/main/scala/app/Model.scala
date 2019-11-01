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

class Model(df: DataFrame) {

  def createModel() = {

    // We create a column with all the ratio for each line
    val dfWithRatio = df.withColumn("ratio", createRatioColumn(df.col("label")))

    // Create a vector with our values  :
    val vector: VectorAssembler = new VectorAssembler()
      .setInputCols(
        Array(
          "appOrSite",
          "bidfloor",
          "exchange",
          "media",
          "network",
          "os",
          "publisher",
          "size",
          "user"
        )
      )
      .setOutputCol("features")

    // WE split our DF for training
    val splitDataFrame: Array[DataFrame] =
      dfWithRatio.randomSplit(Array(0.7, 0.3)) // Need to be confirmed ?
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
  val ratioValue: Double = getRatio()

  def getRatio(): Double = {
    val labelFalse = df.filter(df("label") === false).count
    val totalLabel = df.count
    ((totalLabel - labelFalse).toDouble / totalLabel)
  }

  val createRatioColumn = {
    udf { label: Boolean =>
      if (label) ratioValue
      else 1.0 - ratioValue
    }
  }

}
