import scala.collection.immutable

import sbt.*

object Dependencies {

  def circe(artifact: String): ModuleID =
    "io.circe" %% s"circe-$artifact" % V.circe

  object V {
    val cats              = "2.10.0"
    val catsEffect        = "3.5.2"
    val catsRetry         = "3.1.0"
    val circe             = "0.14.6"
    val fs2               = "3.9.3"
    val fs2Kafka          = "3.2.0"
    val log4cats          = "2.6.0"
    val pureConfig        = "0.17.4"
    val sttp              = "3.8.13"
    var refined           = "0.11.0"
    val parserCombinators = "2.3.0"
    val csv               = "1.3.10"

    val scalaTest      = "3.2.17"
    val testContainers = "0.41.0"

    val betterMonadicFor = "0.3.1"
    val kindProjector    = "0.13.2"
    val logback          = "1.4.11"
  }

  val Cats: Seq[ModuleID] = List(
    "org.typelevel" %% "cats-core" % V.cats
  )

  val CatsEffect: Seq[ModuleID] = List(
    "org.typelevel" %% "cats-effect" % V.catsEffect
  )

  val ApacheCommonsText: Seq[ModuleID] = List(
    "org.apache.commons" % "commons-text" % "1.11.0"
  )

  val ApacheCommons: Seq[ModuleID] = List(
    "org.apache.commons" % "commons-text"  % "1.10.0",
    "commons-codec"      % "commons-codec" % "1.16.0",
    "commons-io"         % "commons-io"    % "2.15.0",
    "org.xerial.snappy"  % "snappy-java"   % "1.1.10.5"
  )

  val ScalaCsv: Seq[ModuleID] = List(
    "com.github.tototoshi" %% "scala-csv" % V.csv
  )

  val Fs2: Seq[ModuleID] = List(
    "co.fs2" %% "fs2-core" % V.fs2
  )

  val Fs2Io: Seq[ModuleID] = List(
    "co.fs2" %% "fs2-io" % V.fs2
  )

  val Fs2Kafka: Seq[ModuleID] = List(
    "com.github.fd4s" %% "fs2-kafka"        % V.fs2Kafka,
    "com.github.fd4s" %% "fs2-kafka-vulcan" % V.fs2Kafka
  )

  val CirceCore    = circe("core")
  val CirceParser  = circe("parser")
  val CirceRefined = circe("refined")

  val Enumeratum: Seq[ModuleID] = List(
    "com.beachape" %% "enumeratum"       % "1.7.3",
    "com.beachape" %% "enumeratum-circe" % "1.7.3"
  )

  val Log4cats: Seq[ModuleID] = List(
    "org.typelevel" %% "log4cats-slf4j" % V.log4cats
  )

  val Refined: Seq[ModuleID] = List(
    "eu.timepit" %% "refined"      % V.refined,
    "eu.timepit" %% "refined-cats" % V.refined
  )

  val Pureconfig: Seq[ModuleID] = List(
    "com.github.pureconfig" %% "pureconfig-core"        % V.pureConfig,
    "com.github.pureconfig" %% "pureconfig-cats-effect" % V.pureConfig,
    "com.github.pureconfig" %% "pureconfig-enumeratum"  % V.pureConfig,
    "com.github.pureconfig" %% "pureconfig-cats"        % V.pureConfig,
    "eu.timepit"            %% "refined-pureconfig"     % V.refined
  )

  val Scopt: Seq[ModuleID] = List("com.github.scopt" %% "scopt" % "4.1.0")

  val FS2Throttler: Seq[ModuleID] = List(
    "dev.kovstas" %% "fs2-throttler" % "1.0.6"
  )

  val Sttp: Seq[ModuleID] = List(
    "com.softwaremill.sttp.client3" %% "core"                           % V.sttp,
    "com.softwaremill.sttp.client3" %% "async-http-client-backend-cats" % V.sttp
  )

  val ParserCombinators: Seq[ModuleID] = Seq(
    "org.scala-lang.modules" %% "scala-parser-combinators" % V.parserCombinators
  )

  val Circe: Seq[ModuleID] = List(
    CirceCore,
    CirceParser,
    CirceRefined
  )

  val AvroConverter: Seq[ModuleID] = List(
    "tech.allegro.schema.json2avro" % "converter" % "0.2.15"
  )

  val ProtoConverter: Seq[ModuleID] = List(
    "io.confluent"          % "kafka-protobuf-serializer" % "7.5.1",
    "com.google.protobuf"   % "protobuf-java-util"        % "3.22.2",
    "com.thesamet.scalapb" %% "scalapb-runtime"           % "0.11.4"
  )

  val ScalaTest: Seq[ModuleID]      = List("org.scalatest" %% "scalatest" % V.scalaTest % Test)
  val CatsEffectTest: Seq[ModuleID] = List("org.typelevel" %% "cats-effect-testing-scalatest" % "1.5.0" % Test)

  val TestContainers: Seq[ModuleID] = List(
    "com.dimafeng"            %% "testcontainers-scala-scalatest"  % V.testContainers % Test,
    "com.dimafeng"            %% "testcontainers-scala-kafka"      % V.testContainers % Test,
    "com.dimafeng"            %% "testcontainers-scala-postgresql" % V.testContainers % Test,
    "io.github.embeddedkafka" %% "embedded-kafka-schema-registry"  % "7.5.2"          % Test
  )

  // Runtime
  val Logback: Seq[ModuleID] = List("ch.qos.logback" % "logback-classic" % V.logback)
}
