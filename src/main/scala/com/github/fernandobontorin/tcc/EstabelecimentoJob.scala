package com.github.fernandobontorin.tcc

import com.github.fernandobontorin.tcc.args.EstabelecimentoParameters
import com.github.fernandobontorin.tcc.session.SparkSessionWrapper
import com.github.fernandobontorin.tcc.transforms.features.{default_columns, isFarmacia, isSPCapital}
import com.github.fernandobontorin.tcc.transforms.paths
import org.apache.spark.sql.functions.lit

object EstabelecimentoJob extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    val params = EstabelecimentoParameters.parse(args)

    params.inputs.map(file => {
      spark.read.options(
        Map("inferSchema" -> "true", "header" -> "true", "delimiter" -> ";")
      ).csv(file).select(default_columns: _*).withColumn("competencia",
        lit(paths.getIsoDateFrom(file)))
        .where(isFarmacia)
        .where(isSPCapital)
    }).reduce(_.union(_))
      .coalesce(1)
      .write
      .options(
      Map("header" -> "true", "delimiter" -> ",")
    ).csv(params.output)


  }

}
