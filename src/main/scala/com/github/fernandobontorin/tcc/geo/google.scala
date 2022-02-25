package com.github.fernandobontorin.tcc.geo

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.fernandobontorin.tcc.geo.models.google.{GeoJson, Location}
import com.sun.org.slf4j.internal.LoggerFactory
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.{CloseableHttpClient, DefaultHttpRequestRetryHandler, HttpClients}
import sttp.client3._

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
        new DefaultHttpRequestRetryHandler(5, true)
      )
      .setDefaultRequestConfig(
        RequestConfig
          .custom()
          .setConnectTimeout(10000)
          .setConnectionRequestTimeout(10000)
          .setSocketTimeout(10000)
          .build()
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
  ): Location = {

    LoggerFactory.getLogger(getClass).error(s"Query ${route}")
    val response = sttp.client3.basicRequest
      .get(
        uri"https://maps.googleapis.com/maps/api/geocode/json?address=${route.trim}, ${streetNumber.trim} - ${postalCode.trim.reverse
          .padTo(8, '0')
          .reverse} ${city.trim} ${state.trim} ${country.trim}&key=$key"
      )
      .send(HttpURLConnectionBackend())

    om
      .readValue[GeoJson](
        response.body.getOrElse(return Location(0d, 0d)),
        classOf[GeoJson]
      )
      .results
      .head
      .geometry
      .location
  }
}
