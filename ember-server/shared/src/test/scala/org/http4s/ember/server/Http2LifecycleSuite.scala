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

package org.http4s.ember.server

import cats.data.Kleisli
import cats.effect._
import cats.effect.std.Semaphore
import cats.syntax.all._
import com.comcast.ip4s._
import fs2.Chunk
import fs2.Stream
import fs2.io.net._
import org.http4s._
import scodec.bits.ByteVector

import java.io.EOFException
import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration._

class Http2LifecycleSuite extends Http4sSuite {
  test("reset discards flow-blocked DATA so a later response can use connection credit") {
    val app = Kleisli[IO, Request[IO], Response[IO]] { request =>
      IO.pure(
        if (request.uri.path.renderString == "/held")
          Response[IO]().withBodyStream(Stream.chunk(Chunk.array(new Array[Byte](16384))).repeat)
        else Response[IO]().withEntity("recovered")
      )
    }
    withServer(app, 1.second) { (peer, _, _) =>
      for {
        _ <- peer.streamWindow(1024 * 1024)
        _ <- peer.get(1, "/held")
        _ <- peer.exhaustConnectionWindow.timeout(3.seconds)
        _ <- peer.resetAndAwaitAck(1)
        _ <- peer.get(3, "/next")
        _ <- peer.connectionWindow(100)
        body <- peer.body(3).timeout(3.seconds)
      } yield assertEquals(body, "recovered")
    }
  }

  test("RST_STREAM cancels a waiting handler and releases its admission permit") {
    for {
      admission <- Semaphore[IO](1L)
      entered <- Deferred[IO, Unit]
      stopped <- Deferred[IO, Unit]
      app = Kleisli[IO, Request[IO], Response[IO]] { request =>
        admission.permit.use { _ =>
          if (request.uri.path.renderString == "/held")
            (entered.complete(()).void >> IO.never[Response[IO]])
              .guarantee(stopped.complete(()).void)
          else IO.pure(Response[IO](Status.Ok).withEntity("recovered"))
        }
      }
      _ <- withServer(app, 1.second) { (peer, _, _) =>
        for {
          _ <- peer.get(1, "/held")
          _ <- entered.get.timeout(3.seconds)
          _ <- peer.resetAndAwaitAck(1)
          _ <- stopped.get.timeout(2.seconds)
          _ <- peer.get(3, "/next")
          body <- peer.body(3).timeout(3.seconds)
        } yield assertEquals(body, "recovered")
      }
    } yield ()
  }

  test("shutdown sends GOAWAY, skips new pushes, and drains an accepted response") {
    for {
      entered <- Deferred[IO, Unit]
      finish <- Deferred[IO, Unit]
      app = Kleisli[IO, Request[IO], Response[IO]](_ =>
        entered.complete(()).void >> finish.get.as(
          Response[IO](Status.Ok)
            .withEntity("completed during drain")
            .withAttribute(
              org.http4s.ember.core.h2.H2Keys.PushPromises,
              List(Request[fs2.Pure](uri = Uri.unsafeFromString("http://localhost/pushed"))),
            )
        )
      )
      _ <- withServer(app, 5.seconds) { (peer, shutdown, terminated) =>
        (for {
          _ <- peer.get(1, "/held")
          _ <- entered.get.timeout(3.seconds)
          _ <- shutdown
          goAway <- peer.goAway.timeout(2.seconds)
          _ = assertEquals(goAway.payload.take(4).toInt(), 1)
          _ = assertEquals(goAway.payload.drop(4).take(4).toInt(), 0)
          _ <- finish.complete(())
          body <- peer.body(1).timeout(2.seconds)
          _ = assertEquals(body, "completed during drain")
          // The peer keeps its socket open: drain completion must not wait for peer EOF.
          _ <- terminated.timeout(2.seconds)
        } yield ()).guarantee(finish.complete(()).void)
      }
    } yield ()
  }

  test("shutdown flushes terminal HEADERS for every accepted response") {
    for {
      enteredFirst <- Deferred[IO, Unit]
      enteredSecond <- Deferred[IO, Unit]
      finishFirst <- Deferred[IO, Unit]
      finishSecond <- Deferred[IO, Unit]
      app = Kleisli[IO, Request[IO], Response[IO]] { request =>
        val (entered, finish) =
          if (request.uri.path.renderString == "/first") (enteredFirst, finishFirst)
          else (enteredSecond, finishSecond)
        entered.complete(()).void >> finish.get.as(Response[IO](Status.NoContent))
      }
      _ <- withServer(app, 5.seconds) { (peer, shutdown, terminated) =>
        (for {
          _ <- peer.get(1, "/first")
          _ <- peer.get(3, "/second")
          _ <- enteredFirst.get.timeout(3.seconds)
          _ <- enteredSecond.get.timeout(3.seconds)
          _ <- shutdown
          goAway <- peer.goAway.timeout(2.seconds)
          _ = assertEquals(goAway.payload.take(4).toInt(), 3)
          _ <- finishFirst.complete(())
          first <- peer.body(1).timeout(2.seconds)
          _ = assertEquals(first, "")
          _ <- finishSecond.complete(())
          second <- peer.body(3).timeout(2.seconds)
          _ = assertEquals(second, "")
          _ <- terminated.timeout(2.seconds)
        } yield ()).guarantee(finishFirst.complete(()).void >> finishSecond.complete(()).void)
      }
    } yield ()
  }

  test("shutdown grace timeout cancels an unfinished handler") {
    for {
      entered <- Deferred[IO, Unit]
      stopped <- Deferred[IO, Unit]
      app = Kleisli[IO, Request[IO], Response[IO]](_ =>
        (entered.complete(()).void >> IO.never[Response[IO]])
          .guarantee(stopped.complete(()).void)
      )
      _ <- withServer(app, 1.second) { (peer, shutdown, terminated) =>
        for {
          _ <- peer.get(1, "/held")
          _ <- entered.get.timeout(3.seconds)
          _ <- shutdown
          _ <- peer.goAway.timeout(2.seconds)
          _ <- terminated.timeout(3.seconds)
          _ <- stopped.get.timeout(1.second)
        } yield ()
      }
    } yield ()
  }

  private def withServer(app: HttpApp[IO], grace: FiniteDuration)(
      test: (Peer, IO[Unit], IO[Unit]) => IO[Unit]
  ): IO[Unit] =
    for {
      ready <- Deferred[IO, SocketAddress[Host]]
      shutdown <- Deferred[IO, Unit]
      server = EmberServerBuilder
        .default[IO]
        .withHost(host"127.0.0.1")
        .withPort(port"0")
        .withHttp2
        .withHttpApp(app)
        .withShutdownTimeout(grace)
        .build
      _ <- Resource
        .make(
          server.use(s => ready.complete(s.addressIp4s).void >> shutdown.get).start
        )(_.cancel)
        .use { running =>
          ready.get.timeout(3.seconds).flatMap { address =>
            Network[IO].connect(address).use { socket =>
              val peer = new Peer(socket)
              peer.initialize >> test(peer, shutdown.complete(()).void, running.joinWithNever)
            }
          }
        }
    } yield ()

  private final case class Frame(kind: Int, flags: Int, streamId: Int, payload: ByteVector)

  private final class Peer(socket: Socket[IO]) {
    private val pingPayload = ByteVector.fromValidHex("0102030405060708")

    def initialize: IO[Unit] =
      socket.write(Chunk.array("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(UTF_8))) >>
        write(Frame(4, 0, 0, ByteVector.empty))

    def get(streamId: Int, path: String): IO[Unit] = {
      val authority = "localhost".getBytes(UTF_8)
      val target = path.getBytes(UTF_8)
      val headers = ByteVector(0x82.toByte, 0x86.toByte, 0x01.toByte, authority.length.toByte) ++
        ByteVector.view(authority) ++ ByteVector(0x04.toByte, target.length.toByte) ++
        ByteVector.view(target)
      write(Frame(1, 5, streamId, headers))
    }

    def resetAndAwaitAck(streamId: Int): IO[Unit] =
      write(Frame(3, 0, streamId, ByteVector.fromInt(8))) >>
        write(Frame(6, 0, 0, pingPayload)) >> awaitPingAck

    def streamWindow(size: Int): IO[Unit] =
      write(Frame(4, 0, 0, ByteVector(0.toByte, 4.toByte) ++ ByteVector.fromInt(size)))

    def connectionWindow(size: Int): IO[Unit] =
      write(Frame(8, 0, 0, ByteVector.fromInt(size)))

    def exhaustConnectionWindow: IO[Unit] = {
      def consume(remaining: Long): IO[Unit] =
        if (remaining == 0) IO.unit
        else
          read.flatMap { frame =>
            if (frame.kind == 0) consume(remaining - frame.payload.size)
            else acknowledge(frame) >> consume(remaining)
          }
      consume(65535L)
    }

    def goAway: IO[Frame] = read.flatMap { frame =>
      if (frame.kind == 7) IO.pure(frame)
      else acknowledge(frame) >> goAway
    }

    def body(streamId: Int): IO[String] = {
      def collect(bytes: ByteVector): IO[String] = read.flatMap { frame =>
        if (frame.streamId == streamId && frame.kind == 3)
          IO.raiseError(new AssertionError(s"accepted stream $streamId was reset"))
        else if (frame.streamId == streamId && (frame.kind == 0 || frame.kind == 1)) {
          val next = if (frame.kind == 0) bytes ++ frame.payload else bytes
          if ((frame.flags & 1) != 0) IO.pure(new String(next.toArray, UTF_8))
          else collect(next)
        } else acknowledge(frame) >> collect(bytes)
      }
      collect(ByteVector.empty)
    }

    private def awaitPingAck: IO[Unit] = read
      .flatMap { frame =>
        if (frame.kind == 6 && (frame.flags & 1) != 0 && frame.payload == pingPayload) IO.unit
        else acknowledge(frame) >> awaitPingAck
      }
      .timeout(3.seconds)

    private def acknowledge(frame: Frame): IO[Unit] =
      if (frame.kind == 4 && (frame.flags & 1) == 0)
        write(Frame(4, 1, 0, ByteVector.empty))
      else if (frame.kind == 6 && (frame.flags & 1) == 0)
        write(Frame(6, 1, 0, frame.payload))
      else if (frame.kind == 7 && frame.payload.drop(4).take(4).toInt() != 0)
        IO.raiseError(new AssertionError(s"connection error GOAWAY: ${frame.payload.toHex}"))
      else IO.unit

    private def write(frame: Frame): IO[Unit] = {
      val length = frame.payload.length.toInt
      val header = ByteVector(
        (length >> 16).toByte,
        (length >> 8).toByte,
        length.toByte,
        frame.kind.toByte,
        frame.flags.toByte,
      ) ++ ByteVector.fromInt(frame.streamId)
      socket.write(Chunk.byteVector(header ++ frame.payload))
    }

    private def read: IO[Frame] =
      for {
        header <- readExactly(9)
        length = ((header(0) & 0xff) << 16) | ((header(1) & 0xff) << 8) | (header(2) & 0xff)
        payload <- readExactly(length)
      } yield Frame(
        header(3) & 0xff,
        header(4) & 0xff,
        header.drop(5).toInt() & 0x7fffffff,
        payload,
      )

    private def readExactly(length: Int): IO[ByteVector] =
      if (length == 0) IO.pure(ByteVector.empty)
      else
        socket.readN(length).flatMap { bytes =>
          if (bytes.size == length) IO.pure(bytes.toByteVector)
          else IO.raiseError(new EOFException("HTTP/2 connection closed within a frame"))
        }
  }
}
