package agh.wggios.analizadanych.datatransform
import agh.wggios.analizadanych
import org.apache.spark.sql.DataFrame
import agh.wggios.analizadanych.{LoggingUtils, SparkSessionProvider}
import org.apache.spark.sql.DataFrame

import java.nio.file.{Files, Paths}
import org.apache.spark.sql.functions._

class DataTransform extends SparkSessionProvider {

  def transform(df: DataFrame): DataFrame = {
    logInfo("Data Transform")

    df.filter("cols IS NOT NULL")
      .withColumn("new_col", upper(col("cols")))
      .select("cols", "cols")
  }
}
