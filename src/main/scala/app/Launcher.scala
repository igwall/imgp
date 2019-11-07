package app
import org.apache.spark.SparkContext
import scala.io.StdIn.readLine
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions.udf
//import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

object Launcher extends App {

  // EXtract all datas for learning
  //val rawData: DataFrame = spark.read.json("src/data/data-students.json")

  println("\nPlease enter the path of the JSON file that you want to predict")
  val filePath: String = readLine().trim()
  Prediction.predict(filePath, "./savedModel")
  // ETL PROCESS
  //val newDf = ETL.cleaningProcess(rawData, true)
  // ===== Create the model : =====

  // Calc the ratio for each label :
  //LogisticModel.createModel(newDf)

  //spark.close()

}
