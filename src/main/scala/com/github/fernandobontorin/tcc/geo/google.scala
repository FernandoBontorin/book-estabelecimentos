package com.github.fernandobontorin.tcc.geo

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.fernandobontorin.tcc.geo.models.google.GeoJson
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.{DefaultHttpRequestRetryHandler, HttpClients}
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types.StringType

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

object google {

  lazy val om: ObjectMapper = {
    new ObjectMapper().registerModule(DefaultScalaModule).disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
  }

  lazy val httpClient = HttpClients.custom().setRetryHandler(new DefaultHttpRequestRetryHandler(Int.MaxValue, true)).build()

  val getGeoPosUDF: UserDefinedFunction = udf(
    (
        key: String,
        flag: String,
        route: String,
        streetNumber: String,
        postalCode: String
    ) => getGeoPosFlag(key, flag, route, streetNumber, postalCode)
  )

  def latitude(key: String): Column =
    getGeoPosUDF(
      lit(key),
      lit("lat"),
      col("NO_LOGRADOURO"),
      col("NU_ENDERECO").cast(StringType),
      col("CO_CEP").cast(StringType)
    ).cast(StringType).as("LATITUDE")

  def longitude(key: String): Column =
    getGeoPosUDF(
      lit(key),
      lit("lng"),
      col("NO_LOGRADOURO"),
      col("NU_ENDERECO").cast(StringType),
      col("CO_CEP").cast(StringType)
    ).cast(StringType).as("LONGITUDE")

  def getGeoPosFlag(
      key: String,
      flag: String,
      route: String,
      streetNumber: String,
      postalCode: String,
      city: String = "São Paulo",
      state: String = "SP",
      country: String = "Brasil"
  ): Double = {
    val queryParams = "?" +
      Map(
        "address" -> s"${route.trim}, ${streetNumber.trim} - ${postalCode.trim.reverse
          .padTo(8, '0')
          .reverse} ${city.trim} ${state.trim} ${country.trim}",
        "key" -> key
      ).toArray
        .map {
          case (k, v) =>
            s"$k=${URLEncoder.encode(v, StandardCharsets.UTF_8.toString)}"
        }
        .mkString("&")

    val url = "https://maps.googleapis.com/maps/api/geocode/json" + queryParams

    val response = httpClient
      .execute(
        new HttpGet(
          "https://maps.googleapis.com/maps/api/geocode/json" + queryParams
        )
      )
    if (response.getStatusLine.getStatusCode != 200) {
      return -1D
    }

    val location = om
      .readValue[GeoJson](
        scala.io.Source
          .fromInputStream(
            response.getEntity.getContent
          )
          .mkString,
        classOf[GeoJson]
      )
      .results
      .head
      .geometry
      .location

    if (Seq("lat", "latitude").contains(flag.toLowerCase))
      return location.lat
    location.lng

  }
}
