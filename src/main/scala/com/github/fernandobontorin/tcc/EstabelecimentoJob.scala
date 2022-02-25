package com.github.fernandobontorin.tcc

import com.github.fernandobontorin.tcc.args.EstabelecimentoParameters
import com.github.fernandobontorin.tcc.geo.google
import com.github.fernandobontorin.tcc.session.SparkSessionWrapper
import com.github.fernandobontorin.tcc.transforms.features.{
  defaultColumns,
  isFarmacia,
  isSPCapital,
  locationColumns
}
import com.github.fernandobontorin.tcc.transforms.paths
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit

object EstabelecimentoJob extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val params = EstabelecimentoParameters.parse(args)

    /** merge all competencias in a dataframe */
    val allEstabs = params.inputs
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
      })
      .reduce(_.union(_))

    /** create unique dataframes with location */
    val uniqueEstabs = allEstabs
      .select(locationColumns: _*)
      .distinct()
      .collect()
      .toSeq
      .map(row => {
        val location = google.getLocation(
          params.googleApisToken,
          row.getAs[String]("NO_LOGRADOURO"),
          row.getAs[String]("NU_ENDERECO"),
          row.getAs[String]("CO_CEP")
        )
        (
          row.getAs[String]("CO_UNIDADE"),
          row.getAs[String]("CO_CNES"),
          location.lat,
          location.lng
        )
      })
      .toDF("CO_UNIDADE", "CO_CNES", "LATITUDE", "LONGITUDE")

    /** join all estabs with its latitude and longitude */
    allEstabs
      .join(uniqueEstabs, Seq("CO_UNIDADE", "CO_CNES"), "left")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .options(
        Map("header" -> "true", "delimiter" -> ",")
      )
      .csv(params.output)

  }

}
