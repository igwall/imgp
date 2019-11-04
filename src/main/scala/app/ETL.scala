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
    val addColIAB = (num: Int, col: org.apache.spark.sql.Column) => {
      val addColUdf = udf { interest: String =>
        if (interest.contains(s"IAB$num")) 1.0
        else 0.0
      }
      addColUdf(col)
    }
    val interestColumn = cleanSubCategories(df.col("interests"))
    val cleaned = df.withColumn("interests", transformUDF(interestColumn))
    val IAB1 = cleaned.withColumn("interest-IAB1", addColIAB(1, cleaned("interests")))
    val IAB2 =  IAB1  .withColumn("interest-IAB2", addColIAB(2, IAB1("interests")))
    val IAB3 =  IAB2  .withColumn("interest-IAB3", addColIAB(3, IAB2("interests")))
    val IAB4 =  IAB3  .withColumn("interest-IAB4", addColIAB(4, IAB3("interests")))
    val IAB5 =  IAB4  .withColumn("interest-IAB5", addColIAB(5, IAB4("interests")))
    val IAB6 =  IAB5  .withColumn("interest-IAB6", addColIAB(6, IAB5("interests")))
    val IAB7 =  IAB6  .withColumn("interest-IAB7", addColIAB(7, IAB6("interests")))
    val IAB8 =  IAB7  .withColumn("interest-IAB8", addColIAB(8, IAB7("interests")))
    val IAB9 =  IAB8  .withColumn("interest-IAB9", addColIAB(9, IAB8("interests")))
    val IAB10 = IAB9  .withColumn("interest-IAB10", addColIAB(10, IAB9("interests")))
    val IAB11 = IAB10.withColumn("interest-IAB11", addColIAB(11, IAB10("interests")))
    val IAB12 = IAB11.withColumn("interest-IAB12", addColIAB(12, IAB11("interests")))
    val IAB13 = IAB12.withColumn("interest-IAB13", addColIAB(13, IAB12("interests")))
    val IAB14 = IAB13.withColumn("interest-IAB14", addColIAB(14, IAB13("interests")))
    val IAB15 = IAB14.withColumn("interest-IAB15", addColIAB(15, IAB14("interests")))
    val IAB16 = IAB15.withColumn("interest-IAB16", addColIAB(16, IAB15("interests")))
    val IAB17 = IAB16.withColumn("interest-IAB17", addColIAB(17, IAB16("interests")))
    val IAB18 = IAB17.withColumn("interest-IAB18", addColIAB(18, IAB17("interests")))
    val IAB19 = IAB18.withColumn("interest-IAB19", addColIAB(19, IAB18("interests")))
    val IAB20 = IAB19.withColumn("interest-IAB20", addColIAB(20, IAB19("interests")))
    val IAB21 = IAB20.withColumn("interest-IAB21", addColIAB(21, IAB20("interests")))
    val IAB22 = IAB21.withColumn("interest-IAB22", addColIAB(22, IAB21("interests")))
    val IAB23 = IAB22.withColumn("interest-IAB23", addColIAB(23, IAB22("interests")))
    val IAB24 = IAB23.withColumn("interest-IAB24", addColIAB(24, IAB23("interests")))
    val IAB25 = IAB24.withColumn("interest-IAB25", addColIAB(25, IAB24("interests")))
                IAB25.withColumn("interest-IAB26", addColIAB(26, IAB25("interests")))
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
}
