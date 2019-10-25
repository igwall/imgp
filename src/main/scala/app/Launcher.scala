import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

object Launcher extends App {
  // Load data here :
  val spark =
    SparkSession.builder
      .master("local[*]")
      .appName("imgp")
      .getOrCreate()

  val rawData: DataFrame = spark.read.json("src/data/data-students.json")
  rawData.show(10)
  val summary: MultivariateStatisticalSummary = Statistics(rawData)
  println(summary.mean)
  //rawData.select($"npaHeaderData.npaNumber".as("npaNumber"))
  //val someDF = spark.createDataFrame(spark.sparkContext.parallelize(rawData))

}
