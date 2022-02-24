package com.github.fernandobontorin.tcc.geo

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.fernandobontorin.tcc.geo.models.google.GeoJson
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.{CloseableHttpClient, DefaultHttpRequestRetryHandler, HttpClients}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

object google {

  val om: ObjectMapper = {
    new ObjectMapper()
      .registerModule(DefaultScalaModule)
      .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
  }

  val httpClient: CloseableHttpClient =
    HttpClients
      .custom()
      .setRetryHandler(
        new DefaultHttpRequestRetryHandler(10, true)
      )
      .build()

  def getLocation(
      key: String,
      route: String,
      streetNumber: String,
      postalCode: String,
      city: String = "São Paulo",
      state: String = "SP",
      country: String = "Brasil"
  ): Seq[Double] = {
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
          url
        )
      )
    if (response.getStatusLine.getStatusCode != 200) {
      return Seq(-1d, -1d)
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
    Seq(location.lat, location.lng)
  }
}
