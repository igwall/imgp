package app
import org.apache.spark.SparkContext
import scala.io.StdIn.readLine
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions.udf
import java.io.File
//import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

object Launcher {

  def main(args: Array[String]) {
    args.toList match {
      case "predict" :: Nil =>
        deleteOldPredictions()
        println("\nPlease enter the path of the JSON file that you want to predict")
        val filePath: String = readLine().trim()
        Prediction.predict(filePath, "./savedModel")
      case "learn" :: Nil =>
        val spark = SparkSession.builder
            .config("spark.master", "local")
            .appName("imgp")
            .getOrCreate()
         //EXtract all datas for learning
        val rawData: DataFrame = spark.read.json("src/data/small.json")

         //ETL PROCESS
        val newDf = ETL.cleaningProcess(rawData, true)
         //===== Create the model : =====

         //Calc the ratio for each label :
        LogisticModel.createModel(newDf)

        spark.close()
      case _ => println("To launch progam use argument predict or learn")
    }
  }

  def deleteOldPredictions(): Unit = {
    def deleteRecursively(file: File) {
      if (file.isDirectory) {
        file.listFiles.foreach(deleteRecursively)
      }
      if (file.exists && !file.delete) {
        throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
      }
    }
    deleteRecursively(new File("predictions"))
  }

}
