package com.github.fernandobontorin.tcc.session

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = {
    SparkSession.builder().getOrCreate()
  }

}
