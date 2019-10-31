package app

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions.udf
import app.ETL
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
  val appOrSite = rawData.select("appOrSite")
  val newDf = ETL.cleaningProcess(rawData)
  println(newDf.select("os").distinct.show(100))
}
