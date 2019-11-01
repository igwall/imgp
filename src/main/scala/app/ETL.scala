package app

import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.types.IntegerType
object ETL {

  def cleaningProcess(df: DataFrame): DataFrame = {
    val appOrSiteCleaned = cleanAppOrSite(df)
    val oscleaned = cleanOS(appOrSiteCleaned)
    val sizeCleaned = cleanSize(oscleaned)
    val publishCleaned = cleanPublisher(sizeCleaned)
    val typeCleaned = cleanType(publishCleaned)
    val userCleaned = cleanUser(typeCleaned)
    val labelCleaned = cleanLabel(userCleaned)
    val bidfloorCleaned = cleanBidfloor(labelCleaned)
    bidfloorCleaned
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

  // Clean OS values
  def cleanOS(df: DataFrame): DataFrame = {

    val transformUDF = udf { os: String =>
      if (os.toLowerCase() == "android") "android"
      else if (os.toLowerCase() == "ios") "ios"
      else if (os == "WindowsMobile" || os == "Windows Mobile OS" || os == "WindowsPhone" || os == "Windows Phone OS")
        "windows phone"
      else if (os.toLowerCase() == "windows") "windows"
      else if (os.toLowerCase().contains("bada")) "bada"
      else if (os.toLowerCase().contains("rim")) "rim"
      else if (os.toLowerCase().contains("symbian")) "symbian"
      else if (os.toLowerCase().contains("webos")) "webos"
      else if (os.toLowerCase().contains("blackberry")) "blackberry"
      else if (os.toLowerCase() == "other" || os.toLowerCase() == "Unknown")
        "unknown"
      else "unknown"
    }

    val osCleaned = transformUDF(df.col("os"))
    val newDataFrame = df.withColumn("os", osCleaned)
    newDataFrame
  }

  // Transform all null in "unknown" or size column
  def cleanSize(df: DataFrame): DataFrame = {

    val transformUDF = udf { size: String =>
      if (size == "null") "unknown"
      else size
    }

    val casted = df.withColumn("size", df("size").cast("string"))
    casted.withColumn("size", transformUDF(casted.col("size")))
  }

  // Transform all null in "unknown" or publisher column
  def cleanPublisher(df: DataFrame): DataFrame = {
    val transformUDF = udf { publisher: String =>
      if (publisher == "null") "unknown"
      else publisher
    }
    df.withColumn("publisher", transformUDF(df.col("publisher")))
  }

  // Transform all null in "unknown" for type column
  def cleanType(df: DataFrame): DataFrame = {
    val transformUDF = udf { elem: String =>
      if (elem == null) "unknown"
      else elem
    }
    df.withColumn("type", transformUDF(df.col("type")))
  }

  // Transform all null in "unknown" for user column
  def cleanUser(df: DataFrame): DataFrame = {
    val transformUDF = udf { user: String =>
      if (user == null) "unknown"
      else user
    }
    df.withColumn("user", transformUDF(df.col("user")))
  }

  def cleanLabel(df: DataFrame): DataFrame = {
    val transformUDF = udf { label: Boolean =>
      if (label) 1.0
      else 0.0
    }
    df.withColumn("label", transformUDF(df.col("label")))
  }

  def cleanBidfloor(df: DataFrame): DataFrame = {
    df.na.fill(0, Seq("bidfloor"))
  }
}
