package io.gen4s.test.outputs

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.OptionValues

import com.google.protobuf.util.JsonFormat
import com.google.protobuf.Struct

import cats.data.NonEmptyList
import cats.effect.unsafe.implicits.global
import cats.effect.IO
import cats.implicits.*
import io.gen4s.core.generators.Variable
import io.gen4s.core.streams.GeneratorStream
import io.gen4s.core.templating.{OutputTransformer, SourceTemplate, TemplateBuilder}
import io.gen4s.core.Domain.*
import io.gen4s.generators.impl.{IntNumberGenerator, StringPatternGenerator}
import io.gen4s.outputs.{KafkaProtobufOutput, OutputStreamExecutor, ProtobufConfig}
import io.github.embeddedkafka.schemaregistry.{EmbeddedKafka, EmbeddedKafkaConfig}

import eu.timepit.refined.types.string.NonEmptyString

class KafkaProtobufOutputStreamTest
    extends AnyFunSpec
    with Matchers
    with KafkaConsumers
    with EmbeddedKafka
    with OptionValues {

  private implicit val config: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(
      kafkaPort = 9092,
      zooKeeperPort = 7001,
      schemaRegistryPort = 8081
    )

  private val kafka = BootstrapServers(s"localhost:${config.kafkaPort}")

  private val n = NumberOfSamplesToGenerate(1)

  case class PersonKey(id: Int, orgId: Int)
  case class Person(username: String, age: Option[Int])

  describe("Kafka Protobuf output stream") {

    it("Convert JSON to Proto message") {
      val structBuilder = Struct.newBuilder
      JsonFormat
        .parser()
        .ignoringUnknownFields()
        .merge("""{ "username": "Den", "age": 38 }""", structBuilder)

      val struct = structBuilder.build()
      println(struct)
    }

    it("Send Protobuf records to kafka topic (auto schema register enabled)") {
      withRunningKafka {
        val template = SourceTemplate("""{ "username": "{{name}}", "age": {{age}} }""")

        val streams = OutputStreamExecutor.make[IO]()

        val builder = TemplateBuilder.make(
          NonEmptyList.one(template),
          List(
            StringPatternGenerator(Variable("name"), NonEmptyString.unsafeFrom("username_###")),
            IntNumberGenerator(Variable("age"), max = 50.some)
          ),
          Nil,
          Set.empty[OutputTransformer]
        )

        val output =
          KafkaProtobufOutput(
            topic = Topic("person"),
            bootstrapServers = kafka,
            ProtobufConfig(
              schemaRegistryUrl = s"http://localhost:${config.schemaRegistryPort}",
              keySchema = None,
              valueSchema = java.io.File("./outputs/src/test/resources/person-value.proto").some,
              autoRegisterSchemas = true
            )
          )

        val list = (for {
          _ <- streams.write(n, GeneratorStream.stream[IO](n, builder), output)
          r <- consumeAllAsMessages[IO](
                 output.topic,
                 kafka,
                 count = n.value.toLong
               )
        } yield r).unsafeRunSync()

        list.foreach(p => info(p.toString))
        list should not be empty
      }
    }

  }

}
