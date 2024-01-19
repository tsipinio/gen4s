package io.gen4s

import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger

import cats.effect.*
import cats.effect.std.Console
import cats.implicits.*
import io.gen4s.cli.*
import io.gen4s.conf.*
import io.gen4s.scenario.ScenarioExecutor
import io.gen4s.stage.StageExecutor

import fs2.io.file.Files
import pureconfig.error.ConfigReaderException

object App extends IOApp {

  implicit def unsafeLogger[F[_]: Sync]: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  override def run(args: List[String]): IO[ExitCode] =
    new CliArgsParser().parse(args, Args()) match {
      case Some(a) =>
        program[IO](a)
          .handleErrorWith {
            case ConfigReaderException(errors) =>
              errors.toList
                .map(e => IO.println(e.origin) *> IO.println(redOut(e.description)))
                .sequence
                .as(ExitCode.Error)

            case TemplateValidationError(errors) => errors.map(e => IO.println(redOut(e))).sequence.as(ExitCode.Error)

            case ex =>
              val err = Option(ex.getMessage).getOrElse("unknown")
              IO.println(redOut(err)).as(ExitCode.Error)
          }
      case None => IO(ExitCode.Error)
    }

  private def program[F[_]: Async: Console: Files](args: Args) =
    for {
      logger         <- Slf4jLogger.create[F]
      _              <- logger.info("Running data generation stream")
      _              <- logger.info(s"Configuration file: ${args.configFile.getAbsolutePath}")
      _              <- logger.info(s"${args.mode.entryName}")
      envVarsProfile <- loadEnvVarsProfile[F](args.profileFile)
      _              <- Async[F].whenA(args.mode == ExecMode.Run)(runStage(args, envVarsProfile))
      _              <- Async[F].whenA(args.mode == ExecMode.Preview)(runStage(args, envVarsProfile))
      _              <- Async[F].whenA(args.mode == ExecMode.RunScenario)(runScenario(args, envVarsProfile))
    } yield ExitCode.Success

  private def runStage[F[_]: Async: Console: Files: Logger](args: Args, envVarsProfile: EnvProfileConfig) = {
    for {
      conf     <- StageConfigLoader.fromFile[F](args.configFile).withEnvProfile(envVarsProfile)
      executor <- StageExecutor.make[F]("Stage", args, conf)
      _        <- Async[F].whenA(args.mode == ExecMode.Run)(executor.exec())
      _        <- Async[F].whenA(args.mode == ExecMode.Preview)(executor.preview())
    } yield ()
  }

  private def runScenario[F[_]: Async: Console: Files](args: Args, envVarsProfile: EnvProfileConfig) = {
    for {
      conf     <- ScenarioConfigLoader.fromFile[F](args.configFile).withEnvProfile(envVarsProfile)
      executor <- ScenarioExecutor.make[F](args, conf, envVarsProfile)
      _        <- Async[F].whenA(args.mode == ExecMode.RunScenario)(executor.exec())
    } yield ()
  }

  private def loadEnvVarsProfile[F[_]: Sync](in: Option[java.io.File]): F[EnvProfileConfig] = in match {
    case Some(f) =>
      val loader = EnvironmentVariablesProfileLoader.make[F]()
      for {
        p <- loader.fromFile(f)
        _ <- loader.unsafeApplyProfile(p)
      } yield p.source

    case None => Sync[F].pure(EnvProfileConfig.empty)
  }

  private def redOut(msg: String): String = s"${scala.Console.RED}- $msg${scala.Console.RESET}"
}
