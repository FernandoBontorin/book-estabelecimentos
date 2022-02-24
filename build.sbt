name := "book-estabelecimentos"

version := "0.1.2"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2" % "provided"

libraryDependencies += "com.github.mrpowers" %% "spark-daria" % "1.0.0"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.1"
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.11" % "test"

assembly / mainClass := Some("com.github.fernandobontorin.tcc.EstabelecimentoJob")
