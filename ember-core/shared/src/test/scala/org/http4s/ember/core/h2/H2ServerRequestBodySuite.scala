/*
 * Copyright 2019 http4s.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.http4s.ember.core.h2

import cats.data.Kleisli
import cats.data.NonEmptyList
import cats.effect._
import cats.effect.std.Queue
import cats.effect.std.Semaphore
import cats.effect.testkit.TestControl
import cats.syntax.all._
import com.comcast.ip4s._
import fs2.Chunk
import fs2.Pipe
import fs2.Stream
import fs2.io.net.Socket
import fs2.io.net.SocketOption
import org.http4s.Http4sSuite
import org.http4s.Response
import org.http4s.Status
import org.http4s.syntax.literals._
import org.typelevel.log4cats.noop.NoOpFactory
import scodec.bits.ByteVector

import scala.concurrent.duration._

class H2ServerRequestBodySuite extends Http4sSuite {
  private val addr = SocketAddress(ip"127.0.0.1", port"0")
  private val settings = H2Frame.Settings.ConnectionSettings.default
  private val ping = H2Frame.Ping(0, ack = false, ByteVector.fromValidHex("0102030405060708"))

  private def frameBytes(frames: List[H2Frame]): ByteVector =
    frames.foldLeft(ByteVector.empty)((bytes, frame) => bytes ++ H2Frame.toByteVector(frame))

  private def decodeFrames(bytes: ByteVector): List[H2Frame] = {
    @annotation.tailrec
    def loop(remaining: ByteVector, frames: List[H2Frame]): List[H2Frame] =
      if (remaining.isEmpty) frames.reverse
      else
        H2Frame.RawFrame.fromByteVector(remaining) match {
          case Some((raw, tail)) =>
            H2Frame.fromRaw(raw) match {
              case Right(frame) => loop(tail, frame :: frames)
              case Left(error) => fail(s"Invalid server frame: $error")
            }
          case None => fail("The server wrote an incomplete frame")
        }
    loop(bytes, Nil)
  }

  private def socket(
      input: Queue[IO, ByteVector],
      output: Queue[IO, H2Frame],
      observed: Ref[IO, Vector[H2Frame]],
  ): Socket[IO] = new Socket[IO] {
    def read(maxBytes: Int): IO[Option[Chunk[Byte]]] = input.take.flatMap { bytes =>
      IO(assert(bytes.size <= maxBytes.toLong)).as(Some(Chunk.byteVector(bytes)))
    }
    def readN(numBytes: Int): IO[Chunk[Byte]] = read(numBytes).map(_.getOrElse(Chunk.empty))
    def reads: Stream[IO, Byte] =
      Stream.repeatEval(read(65535)).unNoneTerminate.flatMap(Stream.chunk)
    def write(bytes: Chunk[Byte]): IO[Unit] = IO(decodeFrames(bytes.toByteVector)).flatMap {
      frames => observed.update(_ ++ frames) >> frames.traverse_(output.offer)
    }
    def writes: Pipe[IO, Byte, Nothing] = _.chunks.evalMap(write).drain
    def endOfInput: IO[Unit] = IO.unit
    def endOfOutput: IO[Unit] = IO.unit
    def isOpen: IO[Boolean] = IO.pure(true)
    def localAddress: IO[SocketAddress[IpAddress]] = IO.pure(addr)
    def remoteAddress: IO[SocketAddress[IpAddress]] = IO.pure(addr)
    def peerAddress: GenSocketAddress = addr
    def address: GenSocketAddress = addr
    def getOption[A](key: SocketOption.Key[A]): IO[Option[A]] = IO.pure(None)
    def setOption[A](key: SocketOption.Key[A], value: A): IO[Unit] = IO.unit
    def supportedOptions: IO[Set[SocketOption.Key[_]]] = IO.pure(Set.empty)
  }

  private def isResponseEnd(streamId: Int)(frame: H2Frame): Boolean = frame match {
    case H2Frame.Data(id, _, _, true) => id == streamId
    case H2Frame.Headers(id, _, true, _, _, _) => id == streamId
    case _ => false
  }

  private def awaitResponseEnd(output: Queue[IO, H2Frame], streamId: Int): IO[Vector[H2Frame]] =
    Stream
      .repeatEval(output.take)
      .takeThrough(frame => !isResponseEnd(streamId)(frame))
      .compile
      .toVector
      .timeoutTo(
        2.seconds,
        IO.raiseError(new AssertionError(s"Connection stalled before response $streamId completed")),
      )

  private def earlyResponse(prefill: Boolean, terminalLast: Boolean): IO[Unit] =
    TestControl.executeEmbed {
      for {
        input <- Queue.unbounded[IO, ByteVector]
        output <- Queue.unbounded[IO, H2Frame]
        observed <- Ref[IO].of(Vector.empty[H2Frame])
        releaseResponse <- Deferred[IO, Unit]
        handlingRequest <- Deferred[IO, Unit]
        requests <- Ref[IO].of(0)
        hpack <- Hpack.create[IO](65535)
        headers <- hpack.encodeHeaders(
          NonEmptyList.of(
            (":method", "POST", false),
            (":scheme", "http", false),
            (":authority", "localhost", false),
            (":path", "/", false),
          )
        )
        laterHeaders <- hpack.encodeHeaders(
          NonEmptyList.of(
            (":method", "GET", false),
            (":scheme", "http", false),
            (":authority", "localhost", false),
            (":path", "/next", false),
          )
        )
        logger <- NoOpFactory[IO].fromClass(classOf[H2ServerRequestBodySuite])
        app = Kleisli { (_: org.http4s.Request[IO]) =>
          requests.update(_ + 1) >> handlingRequest.complete(()).void >>
            releaseResponse.get.as(Response[IO](Status.NoContent))
        }
        lateBody = List.fill(128)(H2Frame.Data(1, ByteVector.empty, None, endStream = false)) :+
          H2Frame.Data(1, ByteVector.empty, None, endStream = terminalLast)
        followup = List(ping, H2Frame.Headers(3, None, true, true, laterHeaders, None))
        _ <- H2Server
          .fromSocket(socket(input, output, observed), app, 1.minute, 1.minute, settings, logger)
          .use(_ => IO.never[Unit])
          .background
          .use { _ =>
            for {
              _ <- input.offer(
                frameBytes(
                  List(
                    H2Frame.Settings.ConnectionSettings.toSettings(settings),
                    H2Frame.Headers(1, None, false, true, headers, None),
                  )
                )
              )
              _ <- handlingRequest.get
              first <-
                if (prefill)
                  for {
                    _ <- input.offer(frameBytes(lateBody ++ followup))
                    // Under virtual time all runnable work settles before the clock advances.
                    // DATA #129 is blocked with no body consumer while the response is gated.
                    _ <- IO.sleep(1.second)
                    before <- observed.get
                    _ = assert(!before.contains(ping.copy(ack = true)), clue(before))
                    _ <- releaseResponse.complete(())
                    completed <- awaitResponseEnd(output, 1)
                  } yield completed
                else
                  for {
                    _ <- releaseResponse.complete(())
                    completed <- awaitResponseEnd(output, 1)
                    _ <- input.offer(frameBytes(lateBody ++ followup))
                  } yield completed
              rest <- awaitResponseEnd(output, 3)
              calls <- requests.get
              frames = first ++ rest
              resets = frames.collect {
                case reset: H2Frame.RstStream if reset.identifier == 1 =>
                  reset
              }
              _ = assert(frames.contains(ping.copy(ack = true)), clue(frames))
              _ = assertEquals(calls, 2)
              _ = assert(
                !frames.exists { case _: H2Frame.GoAway => true; case _ => false },
                clue(frames),
              )
              _ =
                if (terminalLast) assertEquals(resets, Vector.empty)
                else {
                  assertEquals(
                    resets.map(_.value.toInt),
                    Vector(H2Error.NoError.value),
                    clue(frames),
                  )
                  assert(
                    frames.indexWhere(isResponseEnd(1)) < frames.indexOf(resets.head),
                    clue(frames),
                  )
                }
            } yield ()
          }
      } yield ()
    }

  test("an early response discards later DATA without blocking other streams") {
    earlyResponse(prefill = false, terminalLast = false)
  }

  test("an early response releases an already full request body buffer") {
    earlyResponse(prefill = true, terminalLast = false)
  }

  test("response completion releases final DATA blocked after the stream closes") {
    earlyResponse(prefill = true, terminalLast = true)
  }

  private def awaitPing(output: Queue[IO, H2Frame]): IO[Unit] =
    output.take.flatMap { frame =>
      if (frame == ping.copy(ack = true)) IO.unit else awaitPing(output)
    }

  test("a reset before response headers releases the response body and admits another request") {
    TestControl.executeEmbed {
      for {
        input <- Queue.unbounded[IO, ByteVector]
        output <- Queue.unbounded[IO, H2Frame]
        observed <- Ref[IO].of(Vector.empty[H2Frame])
        responseReady <- Deferred[IO, Unit]
        releaseResponse <- Deferred[IO, Unit]
        requests <- Ref[IO].of(0)
        finalized <- Ref[IO].of(0)
        bodyEffects <- Ref[IO].of(0)
        admission <- Semaphore[IO](1)
        hpack <- Hpack.create[IO](65535)
        first <- hpack.encodeHeaders(
          NonEmptyList.of(
            (":method", "GET", false),
            (":scheme", "http", false),
            (":authority", "localhost", false),
            (":path", "/", false),
          )
        )
        next <- hpack.encodeHeaders(
          NonEmptyList.of(
            (":method", "GET", false),
            (":scheme", "http", false),
            (":authority", "localhost", false),
            (":path", "/next", false),
          )
        )
        logger <- NoOpFactory[IO].fromClass(classOf[H2ServerRequestBodySuite])
        app = Kleisli { (_: org.http4s.Request[IO]) =>
          admission.acquire >> requests.updateAndGet(_ + 1).flatMap { requestNumber =>
            val body = Stream
              .eval(bodyEffects.update(_ + 1))
              .as(1.toByte)
              .onFinalize(finalized.update(_ + 1))
              .onFinalize(admission.release)
            val response = Response[IO](Status.Ok).withBodyStream(body)
            if (requestNumber == 1)
              responseReady.complete(()).void >> releaseResponse.get.as(response)
            else IO.pure(response)
          }
        }
        _ <- H2Server
          .fromSocket(socket(input, output, observed), app, 1.minute, 1.minute, settings, logger)
          .use(_ => IO.never[Unit])
          .background
          .use { _ =>
            for {
              _ <- input.offer(
                frameBytes(
                  List(
                    H2Frame.Settings.ConnectionSettings.toSettings(settings),
                    H2Frame.Headers(1, None, true, true, first, None),
                  )
                )
              )
              _ <- responseReady.get
              _ <- input.offer(frameBytes(List(H2Error.Cancel.toRst(1), ping)))
              // The ACK proves the reset has been handled before the response is returned.
              _ <- awaitPing(output)
              _ <- releaseResponse.complete(())
              _ <- input.offer(frameBytes(List(H2Frame.Headers(3, None, true, true, next, None))))
              _ <- awaitResponseEnd(output, 3)
              calls <- requests.get
              releases <- finalized.get
              evaluated <- bodyEffects.get
              available <- admission.available
              _ = assertEquals(calls, 2)
              _ = assertEquals(releases, 2)
              _ = assertEquals(evaluated, 1)
              _ = assertEquals(available, 1L)
            } yield ()
          }
      } yield ()
    }
  }

  private def discardedBody(source: Ref[IO, Int] => Stream[IO, Byte]): IO[Unit] =
    for {
      effects <- Ref[IO].of(0)
      releases <- Ref[IO].of(List.empty[Int])
      body = source(effects)
        .onFinalize(releases.update(1 :: _))
        .onFinalize(releases.update(2 :: _))
      _ <- H2Server.discardResponseBody(body).timeout(2.seconds)
      evaluated <- effects.get
      finalized <- releases.get
      _ = assertEquals(evaluated, 0)
      _ = assertEquals(finalized, List(2, 1))
    } yield ()

  test("discarding a response finalizes nested scopes without running body effects") {
    discardedBody(effects => Stream.eval(effects.update(_ + 1)).as(1.toByte))
  }

  test("discarding a response does not wait for a body that never produces data") {
    discardedBody(effects => Stream.eval(effects.update(_ + 1) >> IO.never[Byte]))
  }

  test("discarding a response terminates a pure infinite body") {
    discardedBody(_ => Stream.constant(1.toByte))
  }

  test("discarding an empty response finalizes nested scopes") {
    discardedBody(_ => Stream.empty)
  }

  test("discarding a response cancels a cancelable resource acquisition") {
    discardedBody(effects =>
      Stream
        .bracketFull[IO, Unit](poll => poll(effects.update(_ + 1) >> IO.never[Unit]))((_, _) =>
          IO.unit
        )
        .as(1.toByte)
    )
  }

  test("canceling before body ownership is transferred releases the response exactly once") {
    for {
      waiting <- Deferred[IO, Unit]
      releases <- Ref[IO].of(0)
      response = Response[IO](Status.Ok).withBodyStream(
        Stream.eval(IO.never[Byte]).onFinalize(releases.update(_ + 1))
      )
      _ <- H2Server
        .responseResource(IO.pure(response))
        .use(_ => waiting.complete(()).void >> IO.never[Unit])
        .background
        .use(_ => waiting.get)
      finalized <- releases.get
      _ = assertEquals(finalized, 1)
    } yield ()
  }

  test("push promises send and finalize their own response bodies") {
    TestControl.executeEmbed {
      for {
        input <- Queue.unbounded[IO, ByteVector]
        output <- Queue.unbounded[IO, H2Frame]
        observed <- Ref[IO].of(Vector.empty[H2Frame])
        finalized <- Ref[IO].of(List.empty[String])
        hpack <- Hpack.create[IO](65535)
        headers <- hpack.encodeHeaders(
          NonEmptyList.of(
            (":method", "GET", false),
            (":scheme", "http", false),
            (":authority", "localhost", false),
            (":path", "/", false),
          )
        )
        logger <- NoOpFactory[IO].fromClass(classOf[H2ServerRequestBodySuite])
        app = Kleisli { (request: org.http4s.Request[IO]) =>
          val pushed = request.uri.path.renderString == "/push"
          val name = if (pushed) "push" else "initial"
          val value = if (pushed) 2.toByte else 1.toByte
          val response = Response[IO](Status.Ok).withBodyStream(
            Stream.emit(value).covary[IO].onFinalize(finalized.update(name :: _))
          )
          IO.pure(
            if (pushed) response
            else
              response.withAttribute(
                H2Keys.PushPromises,
                List(
                  org.http4s.Request[fs2.Pure](uri = uri"http://localhost/push")
                ),
              )
          )
        }
        _ <- H2Server
          .fromSocket(socket(input, output, observed), app, 1.minute, 1.minute, settings, logger)
          .use(_ => IO.never[Unit])
          .background
          .use { _ =>
            for {
              _ <- input.offer(
                frameBytes(
                  List(
                    H2Frame.Settings.ConnectionSettings.toSettings(settings),
                    H2Frame.Headers(1, None, true, true, headers, None),
                  )
                )
              )
              frames <- awaitResponseEnd(output, 1)
              promises = frames.collect { case promise: H2Frame.PushPromise => promise }
              _ = assertEquals(promises.size, 1)
              pushedId = promises.head.promisedStreamId
              pushedBody = frames
                .collect {
                  case H2Frame.Data(id, bytes, _, _) if id == pushedId =>
                    bytes
                }
                .foldLeft(ByteVector.empty)(_ ++ _)
              initialBody = frames
                .collect { case H2Frame.Data(1, bytes, _, _) => bytes }
                .foldLeft(ByteVector.empty)(_ ++ _)
              releases <- finalized.get
              _ = assertEquals(pushedBody, ByteVector(2.toByte))
              _ = assertEquals(initialBody, ByteVector(1.toByte))
              _ = assertEquals(releases.sorted, List("initial", "push"))
            } yield ()
          }
      } yield ()
    }
  }

  test("discarding a response finalizes weak scopes with cancellation") {
    for {
      effects <- Ref[IO].of(0)
      releases <- Ref[IO].of(List.empty[cats.effect.kernel.Resource.ExitCase])
      body = Stream
        .eval(effects.update(_ + 1) >> IO.never[Byte])
        .onFinalizeCaseWeak(exit => releases.update(exit :: _))
      _ <- H2Server.discardResponseBody(body).timeout(2.seconds)
      evaluated <- effects.get
      finalized <- releases.get
      _ = assertEquals(evaluated, 0)
      _ = assertEquals(finalized, List(cats.effect.kernel.Resource.ExitCase.Canceled))
    } yield ()
  }
}
