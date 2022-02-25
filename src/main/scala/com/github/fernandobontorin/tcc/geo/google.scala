package com.github.fernandobontorin.tcc.geo

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.fernandobontorin.tcc.geo.models.google.{GeoJson, Location}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.{
  CloseableHttpClient,
  DefaultHttpRequestRetryHandler,
  HttpClients
}

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
    if (
      response.getStatusLine.getStatusCode != 200 || response.getEntity.getContentLength == 1
    ) {
      println(s"ERROR HTTP ${response.getStatusLine.getStatusCode} GET $url")
      return Location(-1d, -1d)
    }

    om
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
  }
}
