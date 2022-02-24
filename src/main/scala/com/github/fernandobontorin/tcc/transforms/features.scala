package com.github.fernandobontorin.tcc.transforms

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.StringType

object features {

  val defaultColumns: Seq[Column] = Seq[Column](
    col("CO_UNIDADE"),
    col("CO_CNES"),
    col("NU_CNPJ_MANTENEDORA"),
    col("TP_PFPJ"),
    col("NIVEL_DEP"),
    col("NO_RAZAO_SOCIAL"),
    col("NO_FANTASIA"),
    col("NO_LOGRADOURO"),
    col("NU_ENDERECO"),
    col("NO_COMPLEMENTO"),
    col("NO_BAIRRO"),
    col("CO_CEP"),
    col("NU_CPF"),
    col("NU_CNPJ"),
    col("CO_ATIVIDADE"),
    col("CO_CLIENTELA"),
    col("TP_UNIDADE"),
    col("CO_ESTADO_GESTOR"),
    col("CO_MUNICIPIO_GESTOR"),
    col("TP_ESTAB_SEMPRE_ABERTO"),
    col("CO_TIPO_UNIDADE"),
    col("NO_FANTASIA_ABREV"),
    col("TP_GESTAO")
  )

  val locationColumns: Seq[Column] = Seq[Column](
    col("CO_UNIDADE"),
    col("CO_CNES"),
    col("NO_LOGRADOURO").cast(StringType),
    col("NU_ENDERECO").cast(StringType),
    col("CO_CEP").cast(StringType)
  )

  def isFarmacia: Column = when(col("TP_UNIDADE") === 43, true).otherwise(false)

  def isSPCapital: Column = when(col("CO_MUNICIPIO_GESTOR") === 355030, true).otherwise(false)

  def isSPEstado: Column = when(col("CO_MUNICIPIO_GESTOR") === 35, true).otherwise(false)

}
