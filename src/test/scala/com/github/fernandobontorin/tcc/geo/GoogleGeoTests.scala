package com.github.fernandobontorin.tcc.geo

import com.github.fernandobontorin.tcc.SparkSessionTestWrapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class GoogleGeoTests
    extends AnyFlatSpec
    with should.Matchers
    with SparkSessionTestWrapper {

  "geo google" should "create a column LATITUDE" in {
    /*val keyG = ""
    val location = google.getLocation(keyG, "Rua Santa Cruz", "81", "04121000")
    println(location)*/
  }

}
