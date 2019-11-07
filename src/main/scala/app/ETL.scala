package app

import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.types.IntegerType
import org.apache.parquet.filter2.predicate.Operators.Column

object ETL {

  def cleaningProcess(df: DataFrame, training: Boolean): DataFrame = {
    var newdf = df
    newdf = cleanNullableValues(newdf)
    newdf = cleanAppOrSite(newdf)
    newdf = cleanOS(newdf)
    newdf = cleanSize(newdf)
    newdf = cleanPublisher(newdf)
    newdf = cleanType(newdf)
    newdf = cleanInterest(newdf)
    newdf = cleanUser(newdf)
    if (training) {
      newdf = cleanLabel(newdf)
    }

    newdf
  }

  def cleanNullableValues(df: DataFrame): DataFrame = {
    var noNullDF = df.na.fill(-1, Seq("appOrSite"))
    noNullDF = noNullDF.na.fill("N/A", Seq("os"))
    noNullDF = noNullDF.na.fill("N/A", Seq("size"))
    noNullDF = noNullDF.na.fill("N/A", Seq("publisher"))
    noNullDF = noNullDF.na.fill("N/A", Seq("type"))
    noNullDF = noNullDF.na.fill(0, Seq("bidfloor"))
    noNullDF = noNullDF.na.fill("N/A", Seq("interests"))
    noNullDF
  }

//  ==== Cleaning process for appOrSite Column ====
  def cleanAppOrSite(df: DataFrame): DataFrame = {
    val transformUDF = udf { appOrSite: String =>
      if (appOrSite == "app") 1.0
      else if (appOrSite == "site") 0.0
      else -1.0
    }

    val newdf = transformUDF(df.col("appOrSite"))
    val newDataFrame = df.withColumn("appOrSite", newdf)
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
        "N/A"
      else if (os == null) "N/A"
      else "N/A"
    }
    val osCleaned = transformUDF(df.col("os"))
    val newDataFrame = df.withColumn("os", osCleaned)
    newDataFrame
  }

  // Transform all null in "N/A" or size column
  def cleanSize(df: DataFrame): DataFrame = {

    val transformUDF = udf { size: String =>
      if (size == "null") "N/A"
      else size
    }

    val casted = df.withColumn("size", df("size").cast("string"))
    casted.withColumn("size", transformUDF(casted.col("size")))
  }

  // Clean all subcatégories
  // Clean catégories that are not IAB like
  def cleanInterest(df: DataFrame): DataFrame = {

    val transformUDF = udf { interest: String =>
      val splitted = interest.split(",")
      splitted.filter(_.contains("IAB")).mkString(",")
    }

    val interestColumn = cleanSubCategories(df.col("interests"))
    df.withColumn("interests", transformUDF(interestColumn))

  }

  def cleanSubCategories(
      col: org.apache.spark.sql.Column
  ): org.apache.spark.sql.Column = {
    regexp_replace(col, "-[0-9]", "")
  }

  // Transform all null in "N/A" or publisher column
  def cleanPublisher(df: DataFrame): DataFrame = {
    val transformUDF = udf { publisher: String =>
      if (publisher == "null") "N/A"
      else publisher
    }
    df.withColumn("publisher", transformUDF(df.col("publisher")))
  }

  // Transform all null in "N/A" for type column
  def cleanType(df: DataFrame): DataFrame = {
    val transformUDF = udf { elem: String =>
      if (elem == "null") "N/A"
      else elem
    }
    df.withColumn("type", transformUDF(df.col("type")))
  }

  // Transform all null in "N/A" for user column
  def cleanUser(df: DataFrame): DataFrame = {
    val transformUDF = udf { user: String =>
      if (user == "null") "N/A"
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

  def uncleanLabel(df:DataFrame) : DataFrame = {
    val transformUDF = udf { label: Double =>
      if (label == 1.0) true
      else false
    }
    df.withColumn("label", transformUDF(df.col("label")))
  }
}
