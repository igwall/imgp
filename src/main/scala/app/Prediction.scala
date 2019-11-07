package app
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.ml.{Pipeline, PipelineModel}
object Prediction {

  def predict(filePath: String, modelFolder: String) {
    val spark =
      SparkSession.builder
        .config("spark.master", "local")
        .appName("imgp")
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // Get data and clean them
    val rawData: DataFrame = spark.read.json(filePath)
    val dataCleaned: DataFrame = ETL.cleaningProcess(rawData, false)

    // Load the model
    val model: PipelineModel = PipelineModel.load(modelFolder)
    // Index the tabs
    val dataIndexed: DataFrame = LogisticModel.indexStringColumns(
      dataCleaned,
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
        "user",
        "city"
      )
    )
    // Launch the prediction with our model
    val predictions: DataFrame = model.transform(dataIndexed)

    // Get all the results from prediction
    var predictionsDf: DataFrame = predictions.select("prediction")
    val finalColumn = predictionsDf
      .withColumn("label", predictionsDf("prediction"))
      .drop("prediction")

    val newDf: DataFrame =
      rawData.withColumn("id1", monotonically_increasing_id())
    val newPredictions: DataFrame =
      finalColumn.withColumn("id2", monotonically_increasing_id())

    // Join the original dataframe with the prediction
    val df2: DataFrame = newDf
      .as("df1")
      .join(
        newPredictions.as("df2"),
        newDf("id1") === newPredictions("id2"),
        "inner"
      )
      .select(
        "df2.label",
        "df1.appOrSite",
        "df1.bidFloor",
        "df1.city",
        "df1.exchange",
        "df1.impid",
        "df1.interests",
        "df1.media",
        "df1.network",
        "df1.os",
        "df1.publisher",
        "df1.size",
        "df1.timestamp",
        "df1.type",
        "df1.user"
      )

    val dfLabelCleaned = ETL.uncleanLabel(df2)
    val dfToExport: DataFrame = ETL.cleanSize(dfLabelCleaned)

    // Export the result as CSV in one file
    dfToExport
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .save("./predictions/prediction.csv")

    spark.close()

  }
}
