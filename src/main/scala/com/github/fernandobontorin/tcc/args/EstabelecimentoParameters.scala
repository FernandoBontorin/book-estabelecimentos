package com.github.fernandobontorin.tcc.args

case class EstabelecimentoParameters(
                                      inputs: Seq[String] = Seq[String](),
                                      output: String = ""
                                    ) extends Serializable


object EstabelecimentoParameters {

  private val parser = new scopt.OptionParser[EstabelecimentoParameters]("book-estabelecimentos") {
    head("book-estabelecimentos", "0.1.0")

    opt[Seq[String]]('i', "inputs")
      .valueName("s3://my-bucket/root.csv")
      .action((value, params) => params.copy(inputs = value))
      .text("set dataframes (CSVs only) to read")
      .required()

    opt[String]('o', "output")
      .valueName("s3://my-bucket/output")
      .action((value, params) => params.copy(output = value))
      .text("set output of book")
      .required()
  }

  def parse(args: Array[String]): EstabelecimentoParameters = {
    parser.parse(args, EstabelecimentoParameters()) match {
      case Some(params) => params
      case _ => throw new IllegalArgumentException("")
    }
  }

}
