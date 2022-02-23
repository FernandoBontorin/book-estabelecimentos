package com.github.fernandobontorin.tcc

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}

trait SparkSessionTestWrapper {

  val spark: SparkSession = {
    Logger.getLogger("org").setLevel(Level.OFF)
    SparkSession.builder()
      .master("local[1]")
      .getOrCreate()
  }

}
