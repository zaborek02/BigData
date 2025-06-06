package agh.wggios.analizadanych.datareader

import agh.wggios.analizadanych
import org.apache.spark.sql.DataFrame
import agh.wggios.analizadanych.{LoggingUtils, SparkSessionProvider}
import org.apache.spark.sql.DataFrame

import java.nio.file.{Files, Paths}

class DataReader extends SparkSessionProvider {

  def read_csv(path: String): DataFrame =  {
    logInfo("File reading")
    spark.read.format("csv").option("header", value = true).option("inferSchema", value = true).load(path)

  }

  def read_parquet(path: String): DataFrame = {
    logInfo("parquet file reading, A CO")
    spark.read.format("parquet").option("header", value = true).option("inferSchema", value = true).load(path)
  }
}
