package com.github.fernandobontorin.tcc.transforms

import com.github.fernandobontorin.tcc.session.SparkSessionWrapper

object paths extends SparkSessionWrapper {

  def getIsoDateFrom(path: String): String = {
    val raw = path.split("[.]").head.takeRight(6)
    s"${raw.substring(0, 4)}-${raw.substring(4, 6)}-01"
  }

}
