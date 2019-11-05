package app
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions.udf
//import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

object Launcher extends App {
  // Load data here :
  val spark =
    SparkSession.builder
      .config("spark.master", "local")
      .appName("imgp")
      .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  // EXtract all datas for learning
  val rawData: DataFrame = spark.read.json("src/data/medium.json")

  // ETL PROCESS
  val newDf = ETL.cleaningProcess(rawData, true)
  // ===== Create the model : =====

  // Calc the ratio for each label :
  LogisticModel.createModel(newDf)
  spark.close()

}
