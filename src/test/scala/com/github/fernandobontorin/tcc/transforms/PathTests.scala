package com.github.fernandobontorin.tcc.transforms

import com.github.fernandobontorin.tcc.SparkSessionTestWrapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class PathTests extends AnyFlatSpec with should.Matchers with SparkSessionTestWrapper {

  "getIsoDateFrom" should "parse path and extract year and month" in {
    assert(paths.getIsoDateFrom("qualquerpath201902.csv").equals("2019-02-01"))
  }

}
