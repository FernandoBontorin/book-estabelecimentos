package com.github.fernandobontorin.tcc.geo

import com.github.fernandobontorin.tcc.SparkSessionTestWrapper
import org.apache.spark.sql.functions.{col, lpad}
import org.apache.spark.sql.types.StringType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class GoogleGeoTests
    extends AnyFlatSpec
    with should.Matchers
    with SparkSessionTestWrapper {

  "geo google" should "create a column LATITUDE" in {
    /*
    val keyG = ""
    import spark.implicits._
    val df = Seq(("AV BRIGADEIRO LUIZ ANTONIO", "683", "1317000")).toDF(
      Seq("NO_LOGRADOURO", "NU_ENDERECO", "CO_CEP"): _*
    )

    val dfGeo = df.select(
      df.columns.map(col) ++ Seq(
        google.latitude(keyG), google.longitude(keyG)): _*
    )
    dfGeo.show()
    */
  }

}
