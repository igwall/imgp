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
      .master("local[*]")
      .appName("imgp")
      .getOrCreate()

  // EXtract all datas for learning
  val rawData: DataFrame = spark.read.json("src/data/small.json")

  // ETL PROCESS
  val newDf = ETL.cleaningProcess(rawData)

  // ===== Create the model : =====

  // Calc the ratio for each label :
  val model = new Model(newDf)
  model.createModel()

}
