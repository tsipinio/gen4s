package io.gen4s.test

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import cats.effect.kernel.Sync
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.IO
import io.gen4s.conf.*
import io.gen4s.conf.OutputConfig
import io.gen4s.core.Domain
import io.gen4s.outputs.*
import io.gen4s.outputs.StdOutput

import eu.timepit.refined.types.numeric.PosInt
import eu.timepit.refined.types.string.NonEmptyString

class OutputLoaderTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  private def load[F[_]: Sync](str: String): F[OutputConfig] = {
    import pureconfig.*
    import pureconfig.ConfigSource
    import pureconfig.module.catseffect.syntax.*

    ConfigSource
      .string(str)
      .loadF[F, OutputConfig]()
  }

  "Output loader" - {

    "Load std output" in {
      load[IO]("""writer: { type: std-output }
                 |transformers = []
                 |validators = []
                 |""".stripMargin)
        .asserting { out =>
          out.writer shouldBe StdOutput()
        }
    }

    "Load kakfa output" in {
      load[IO]("""
        writer: { 
          type: kafka-output
          topic: test
          bootstrap-servers: "localhost:9092"
          headers: {
              key = value
          }
          batch-size: 1000
          producer-config {
            compression-type = gzip
            in-flight-requests =  1
            linger-ms = 15
            max-batch-size-bytes = 1024
            max-request-size-bytes = 512
          }
       }
       transformers = []
       validators = []
       """.stripMargin)
        .asserting { out =>
          out.writer shouldBe KafkaOutput(
            topic = Domain.Topic("test"),
            bootstrapServers = Domain.BootstrapServers("localhost:9092"),
            headers = Map("key" -> "value"),
            batchSize = PosInt.unsafeFrom(1000),
            producerConfig = Some(KafkaProducerConfig(KafkaProducerConfig.CompressionTypes.gzip, 15L, 1024, 512L, 1))
          )
        }
    }

    "Load http output" in {
      load[IO]("""
        writer: { 
          type: http-output
          url: "http://example.com"
          method: POST
          headers: {
              key = value
          }
          parallelism: 3
          content-type: "application/json"
          stop-on-error: true
       }
       transformers = []
       validators = []
       """.stripMargin)
        .asserting { out =>
          out.writer shouldBe HttpOutput(
            url = "http://example.com",
            method = HttpMethods.Post,
            parallelism = PosInt.unsafeFrom(3),
            headers = Map("key" -> "value"),
            contentType = HttpContentTypes.ApplicationJson,
            stopOnError = true
          )
        }
    }

    "Load file system output" in {
      load[IO]("""
        writer: { 
          type: fs-output
          dir: "/tmp"
          filename-pattern: "my-cool-logs-%s.txt"
       }
       transformers = []
       validators = []
       """.stripMargin)
        .asserting { out =>
          out.writer shouldBe FsOutput(
            dir = NonEmptyString.unsafeFrom("/tmp"),
            filenamePattern = NonEmptyString.unsafeFrom("my-cool-logs-%s.txt")
          )
        }
    }
  }

}
