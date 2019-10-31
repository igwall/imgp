package app

import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions.udf
object ETL {

  def cleaningProcess(df: DataFrame): DataFrame = {
    // val appOrSiteCleaned = cleanAppOrSite(df)
    val oscleaned = cleanOS(df)
    oscleaned
    // appOrSiteCleaned
  }

//  ==== Cleaning process for appOrSite Column ====
  def cleanAppOrSite(df: DataFrame): DataFrame = {
    val transformUDF = udf { appOrSite: String =>
      if (appOrSite == "app") 1.0
      else if (appOrSite == "site") 0.0
      else -1.0
    }
    val appOrSiteCleaned = transformUDF(df.col("appOrSite"))
    val newDataFrame = df.withColumn("appOrSite", appOrSiteCleaned)
    newDataFrame
  }

  // ==== Cleaning OS ====

  def cleanOS(df: DataFrame): DataFrame = {

    val transformUDF = udf { os: String =>
      println(os)
      if (os.toLowerCase() == "android") 1.0
      else if (os.toLowerCase() == "ios") 2.0
      else if (os == "WindowsMobile" || os == "Windows Mobile OS" || os == "WindowsPhone" || os == "Windows Phone OS")
        3.0
      else if (os.toLowerCase() == "windows") 4.0
      else if (os.toLowerCase().contains("bada")) 5.0
      else if (os.toLowerCase().contains("rim")) 6.0
      else if (os.toLowerCase().contains("symbian")) 7.0
      else if (os.toLowerCase().contains("webos")) 8.0
      else if (os.toLowerCase().contains("blackberry")) 9.0
      else if (os.toLowerCase() == "other" || os.toLowerCase() == "Unknown")
        10.0
      else -1.0
    }

    val osCleaned = transformUDF(df.col("os"))
    val newDataFrame = df.withColumn("os", osCleaned)
    newDataFrame
  }
}
