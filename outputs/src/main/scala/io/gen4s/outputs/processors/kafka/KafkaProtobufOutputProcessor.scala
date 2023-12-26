package io.gen4s.outputs.processors.kafka

import cats.effect.kernel.Async
import cats.implicits.*
import cats.Applicative
import io.circe.ParsingFailure
import io.gen4s.core.templating.Template
import io.gen4s.core.Domain
import io.gen4s.core.Domain.NumberOfSamplesToGenerate
import io.gen4s.outputs.processors.OutputProcessor
import io.gen4s.outputs.KafkaProtobufOutput

import fs2.kafka.vulcan.SchemaRegistryClientSettings

class KafkaProtobufOutputProcessor[F[_]: Async]
    extends OutputProcessor[F, KafkaProtobufOutput]
    with KafkaOutputProcessorBase {

  override def process(
    n: Domain.NumberOfSamplesToGenerate,
    flow: fs2.Stream[F, Template],
    output: KafkaProtobufOutput): F[Unit] = {

    val protoConfig = output.protoConfig

    val registryClientSettings = SchemaRegistryClientSettings[F](protoConfig.schemaRegistryUrl)
      .withMaxCacheSize(protoConfig.registryClientMaxCacheSize)

    println(registryClientSettings)

    val groupSize = if (output.batchSize.value > n.value) output.batchSize.value else n.value
    val headers   = KafkaOutputProcessor.toKafkaHeaders(output.headers)

    val producerSettings =
      mkProducerSettingsResource[F, Key, Value](output.bootstrapServers, output.kafkaProducerConfig)

    flow
      .chunkN(groupSize)
      .evalMap { batch =>
        batch
          .map { value =>
            if (output.decodeInputAsKeyValue) {
              value.render().asKeyValue match {
                case Right((key, v)) =>
                  Applicative[F].pure(
                    fs2.kafka
                      .ProducerRecord(output.topic.value, key.asByteArray, v.asByteArray)
                      .withHeaders(headers)
                  )

                case Left(ex) =>
                  Async[F].raiseError(ParsingFailure(s"Template key/value parsing failure: ${ex.message}", ex))
              }

            } else {
              Applicative[F].pure(
                fs2.kafka
                  .ProducerRecord(output.topic.value, null, value.render().asByteArray)
                  .withHeaders(headers)
              )
            }
          }
          .sequence
          .map(fs2.kafka.ProducerRecords.apply)
      }
      .through(fs2.kafka.KafkaProducer.pipe(producerSettings))
      .compile
      .drain
  }
}
