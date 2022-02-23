package com.github.fernandobontorin.tcc

import com.github.fernandobontorin.tcc.args.EstabelecimentoParameters
import com.github.fernandobontorin.tcc.geo.google
import com.github.fernandobontorin.tcc.session.SparkSessionWrapper
import com.github.fernandobontorin.tcc.transforms.features.{
  defaultColumns,
  isFarmacia,
  isSPCapital
}
import com.github.fernandobontorin.tcc.transforms.paths
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit

object EstabelecimentoJob extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    val params = EstabelecimentoParameters.parse(args)

    params.inputs
      .map(file => {
        spark.read
          .options(
            Map("inferSchema" -> "true", "header" -> "true", "delimiter" -> ";")
          )
          .csv(file)
          .select(defaultColumns: _*)
          .withColumn("competencia", lit(paths.getIsoDateFrom(file)))
          .where(isFarmacia)
          .where(isSPCapital)
          .select(
            defaultColumns ++ Seq(
              google.latitude(params.googleApisToken),
              google.longitude(params.googleApisToken)
            ): _*
          )
      })
      .reduce(_.union(_))
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .options(
        Map("header" -> "true", "delimiter" -> ",")
      )
      .csv(params.output)

  }

}
