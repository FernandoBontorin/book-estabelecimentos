package com.github.fernandobontorin.tcc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  val spark: SparkSession = {
    Logger.getLogger("org").setLevel(Level.OFF)
    SparkSession
      .builder()
      .master("local[1]")
      .getOrCreate()
  }

}
