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

import cats.data.NonEmptyList
import cats.effect.Deferred
import cats.effect.IO
import cats.effect.Ref
import cats.effect.std.Queue
import cats.effect.testkit.TestControl
import cats.syntax.all._
import fs2.Chunk
import fs2.Stream
import fs2.concurrent.Channel
import fs2.text.utf8
import org.http4s.Headers
import org.http4s.Http4sSuite
import org.http4s.HttpVersion
import org.http4s.Request
import org.http4s.Response
import org.http4s.Status
import org.typelevel.log4cats
import scodec.bits.ByteVector

import scala.concurrent.duration.DurationLong

class H2StreamSuite extends Http4sSuite {
  val defaultSettings = H2Frame.Settings.ConnectionSettings.default

  private def streamAndQueue(
      config: H2Frame.Settings.ConnectionSettings,
      connectionType: H2Connection.ConnectionType = H2Connection.ConnectionType.Server,
      onClosed: IO[Unit] = IO.unit,
      hpackOverride: Option[Hpack[IO]] = None,
  ): IO[(H2Stream[IO], Queue[IO, Chunk[H2Frame]])] =
    for {
      writeBlock <- Deferred[IO, Either[Throwable, Unit]]
      req <- Deferred[IO, Either[Throwable, Request[fs2.Pure]]]
      resp <- Deferred[IO, Either[Throwable, Response[fs2.Pure]]]
      trailers <- Deferred[IO, Either[Throwable, Headers]]
      readBuffer <- Channel.unbounded[IO, Either[Throwable, ByteVector]]

      state <- Ref[IO].of(
        H2Stream.State[IO](
          state = H2Stream.StreamState.Open,
          writeWindow = defaultSettings.initialWindowSize.windowSize,
          writeBlock = writeBlock,
          readWindow = config.initialWindowSize.windowSize,
          request = req,
          response = resp,
          trailers = trailers,
          readBuffer = readBuffer,
          contentLengthCheck = None,
          stallStart = None,
        )
      )
      hpack <- hpackOverride.fold(Hpack.create[IO](1024))(IO.pure)
      logger <- log4cats.noop.NoOpFactory[IO].fromClass(classOf[H2StreamSuite])
      outgoing <- Queue.unbounded[IO, Chunk[H2Frame]]
      stream = new H2Stream[IO](
        1,
        60.seconds,
        defaultSettings,
        connectionType,
        IO.pure(config),
        state,
        hpack,
        outgoing,
        onClosed,
        _ => IO.unit,
        logger,
      )
    } yield (stream, outgoing)

  def clientStream(
      config: H2Frame.Settings.ConnectionSettings
  ): IO[H2Stream[IO]] =
    for {
      writeBlock <- Deferred[IO, Either[Throwable, Unit]]
      req <- Deferred[IO, Either[Throwable, Request[fs2.Pure]]]
      resp <- Deferred[IO, Either[Throwable, Response[fs2.Pure]]]
      trailers <- Deferred[IO, Either[Throwable, Headers]]
      readBuffer <- Channel.unbounded[IO, Either[Throwable, ByteVector]]

      state <- Ref[IO].of(
        H2Stream.State[IO](
          state = H2Stream.StreamState.Idle,
          writeWindow = defaultSettings.initialWindowSize.windowSize,
          writeBlock = writeBlock,
          readWindow = config.initialWindowSize.windowSize,
          request = req,
          response = resp,
          trailers = trailers,
          readBuffer = readBuffer,
          contentLengthCheck = None,
          stallStart = None,
        )
      )
      hpack <- Hpack.create[IO](1024)
      logger <- log4cats.noop.NoOpFactory[IO].fromClass(classOf[H2StreamSuite])
      enqueue <- Queue.unbounded[IO, Chunk[H2Frame]]
      stream = new H2Stream[IO](
        1,
        60.seconds,
        defaultSettings,
        H2Connection.ConnectionType.Client,
        IO.pure(config),
        state,
        hpack,
        enqueue,
        IO.unit,
        _ => IO.unit,
        logger,
      )
    } yield stream

  private def testMessageSize(
      stream: H2Stream[IO],
      outgoing: Queue[IO, Chunk[H2Frame]],
      frameSize: Int,
      messageSize: Int,
      numFrames: Int,
  ) = {
    val sample = Response[IO](Status.Ok, HttpVersion.`HTTP/2`)
      .withEntity("0" * messageSize)

    for {
      _ <- stream.sendMessageBody(sample)
      chunks <- outgoing.take.replicateA(numFrames).map(_.flatMap(_.toList))
      data = chunks.collect { case H2Frame.Data(_, data, _, _) => data }
      _ <- assertIO(IO(data.size), numFrames)
      _ <- assertIO(IO(data.map(_.size).sum), messageSize.toLong)
      _ <- data.traverse_(c => IO(assert(clue(c.size) <= clue(frameSize))))
    } yield ()
  }

  test("H2Stream sendMessageBody empty message should send one empty Data frame and half-close") {
    val config = defaultSettings

    for {
      sq <- streamAndQueue(config)
      (stream, queue) = sq
      _ <- testMessageSize(stream, queue, 0, messageSize = 0, numFrames = 1)
      _ <- assertIO(stream.state.get.map(_.state), H2Stream.StreamState.HalfClosedLocal)
    } yield ()
  }

  test(
    "client should not hang when endStream is sent by H2Frame.Headers as trailers"
  ) {
    for {
      stream <- clientStream(defaultSettings)
      headers <- stream.hpack.encodeHeaders(
        NonEmptyList.of(
          (":status", "200", false),
          (":method", "GET", false),
        )
      )
      init = H2Frame.Headers(
        0,
        None,
        endStream = false,
        endHeaders = false,
        headers,
        None,
      )
      headers <- stream.hpack.encodeHeaders(
        NonEmptyList.of(
          ("grpc-status", "0", false)
        )
      )
      trailers = H2Frame.Headers(
        1,
        None,
        endStream = true,
        endHeaders = true,
        headers,
        None,
      )

      source = fs2.Stream.repeatEval(IO(42.toByte)).take(10000).chunkN(100)
      actual <- Queue.unbounded[IO, Chunk[Byte]]

      _ <- stream.receiveHeaders(init, List.empty)
      _ <- assertIO(stream.state.get.map(_.state), H2Stream.StreamState.Open)
      _ <- (
        // Taken from `sendMessageBody` to emulate messages sent from server.
        source.zipWithNext
          .foreach { case (c, nextChunk) =>
            val noTrailers = false
            val isEndStream = nextChunk.isEmpty && noTrailers
            stream.receiveData(H2Frame.Data(0, c.toByteVector, None, isEndStream)) >>
              actual.offer(c)
          }
          .compile
          .drain >>
          // Taken from `sendTrailerHeaders` to emulate trailers headers sent from server.
          stream
            .receiveHeaders(trailers, List.empty)
      )
        // Note: Without closing `readBuffer` on headers with `endStream=true`, `readBody` hangs forever.
        .both(stream.readBody.compile.drain)
      expect <- source.compile.count
      _ <- assertIO(
        actual.size,
        expect.toInt,
        "expect the client to consume all the elements in the streaming response body before closing",
      )
      _ <- assertIO(stream.state.get.map(_.state), H2Stream.StreamState.HalfClosedRemote)
    } yield ()
  }

  test(
    "H2Stream sendMessageBody body=16kb frameSize=16kb should send one Data frame and half-close"
  ) {
    val frameSize = 16384
    val config = defaultSettings.copy(
      maxFrameSize = H2Frame.Settings.SettingsMaxFrameSize(frameSize)
    )

    for {
      sq <- streamAndQueue(config)
      (stream, queue) = sq
      _ <- testMessageSize(stream, queue, frameSize, messageSize = frameSize, numFrames = 1)
      _ <- assertIO(stream.state.get.map(_.state), H2Stream.StreamState.HalfClosedLocal)
    } yield ()
  }

  test(
    "H2Stream sendMessageBody body=50kb frameSize=16kb should send four Data frames and half-close"
  ) {
    val frameSize = 16384
    val config = defaultSettings.copy(
      maxFrameSize = H2Frame.Settings.SettingsMaxFrameSize(frameSize)
    )

    for {
      sq <- streamAndQueue(config)
      (stream, queue) = sq
      _ <- testMessageSize(stream, queue, frameSize, messageSize = 51200, numFrames = 4)
      _ <- assertIO(stream.state.get.map(_.state), H2Stream.StreamState.HalfClosedLocal)
    } yield ()
  }

  test(
    "H2Stream sendMessageBody body=50kb frameSize=32kb should send two Data frames and half-close"
  ) {
    val frameSize = 32768
    val config = defaultSettings.copy(
      maxFrameSize = H2Frame.Settings.SettingsMaxFrameSize(frameSize)
    )

    for {
      sq <- streamAndQueue(config)
      (stream, queue) = sq
      _ <- testMessageSize(stream, queue, frameSize, messageSize = 51200, numFrames = 2)
      _ <- assertIO(stream.state.get.map(_.state), H2Stream.StreamState.HalfClosedLocal)
    } yield ()
  }

  test("H2Stream sendMessageBody empty message without 'Trailer' header closes Stream") {
    val config = defaultSettings

    for {
      sq <- streamAndQueue(config)
      (stream, _) = sq
      resp = Response[IO](Status.Ok, HttpVersion.`HTTP/2`)
      _ <- stream.sendMessageBody(resp)
      _ <- assertIO(stream.state.get.map(_.state), H2Stream.StreamState.HalfClosedLocal)
    } yield ()
  }

  test("H2Stream sendMessageBody empty message with 'Trailer' header keeps stream open") {
    val config = defaultSettings

    for {
      sq <- streamAndQueue(config)
      (stream, _) = sq
      resp = Response[IO](Status.Ok, HttpVersion.`HTTP/2`)
        .withTrailerHeaders(IO.pure(Headers("Trailer" -> "Expires")))
      _ <- stream.sendMessageBody(resp)
      _ <- assertIO(stream.state.get.map(_.state), H2Stream.StreamState.Open)
    } yield ()
  }

  test("H2Stream sendMessageBody non-empty message with 'Trailer' header keeps stream open") {
    val frameSize = 16384
    val config = defaultSettings.copy(
      maxFrameSize = H2Frame.Settings.SettingsMaxFrameSize(frameSize)
    )

    for {
      sq <- streamAndQueue(config)
      (stream, _) = sq
      resp = Response[IO](Status.Ok, HttpVersion.`HTTP/2`)
        .withTrailerHeaders(IO.pure(Headers("Trailer" -> "Expires")))
        .withEntity("0" * frameSize * 2)
      _ <- stream.sendMessageBody(resp)
      _ <- assertIO(stream.state.get.map(_.state), H2Stream.StreamState.Open)
    } yield ()
  }

  test(
    "H2Stream sendMessageBody should flush data without waiting for the next chunk"
  ) {

    def bodyStream(gate: Deferred[IO, Unit]): Stream[IO, Byte] =
      Stream("hello").through(utf8.encode) ++
        Stream.eval(gate.get).drain ++
        Stream("world").through(utf8.encode)

    def assertFrame(chunk: Chunk[H2Frame], expected: String, endStream: Boolean) = {
      assert(chunk.size == 1)
      val frame = chunk.collectFirst { case data: H2Frame.Data => data }.get

      assertEquals(frame.data.decodeUtf8, Right(expected))
      assertEquals(frame.endStream, endStream)
    }

    for {
      sq <- streamAndQueue(defaultSettings)
      (stream, queue) = sq
      gate <- Deferred[IO, Unit]
      resp = Response[IO](Status.Ok, HttpVersion.`HTTP/2`).withBodyStream(bodyStream(gate))
      fiber <- stream.sendMessageBody(resp).start
      firstChunk <- queue.take
      _ <- IO(assertFrame(firstChunk, "hello", endStream = false))
      _ <- assertIO(stream.state.get.map(_.state), H2Stream.StreamState.Open)
      _ <- gate.complete(())
      secondChunk <- queue.take
      _ <- IO(assertFrame(secondChunk, "world", endStream = false))
      lastChunk <- queue.take
      _ <- IO(assertFrame(lastChunk, "", endStream = true))
      _ <- fiber.joinWithNever
      _ <- assertIO(stream.state.get.map(_.state), H2Stream.StreamState.HalfClosedLocal)
    } yield ()
  }

  test("sendData clears stallStart when peer grants enough credit for a full chunk") {
    TestControl.executeEmbed {
      for {
        sq <- streamAndQueue(defaultSettings)
        (stream, _) = sq
        _ <- stream.state.update(_.copy(writeWindow = 0))
        fiber <- stream.sendData(ByteVector.fill(10)(0), endStream = false).start
        _ <- IO.sleep(10.seconds)
        stalled <- stream.state.get.map(_.stallStart)
        _ = assert(stalled.isDefined)
        _ <- stream.receiveWindowUpdate(H2Frame.WindowUpdate(1, 10))
        _ <- fiber.joinWithNever
        st <- stream.state.get
      } yield assertEquals(st.stallStart, None)
    }
  }

  test("sendData can end a stream with an empty DATA frame when the window is zero") {
    for {
      sq <- streamAndQueue(defaultSettings)
      (stream, queue) = sq
      _ <- stream.state.update(_.copy(writeWindow = 0))
      _ <- stream.sendData(ByteVector.empty, endStream = true)
      outgoing <- queue.take
      updated <- stream.state.get
      data = outgoing.collectFirst { case frame: H2Frame.Data => frame }
    } yield {
      assertEquals(data.map(_.data), Some(ByteVector.empty))
      assertEquals(data.map(_.endStream), Some(true))
      assertEquals(updated.state, H2Stream.StreamState.HalfClosedLocal)
    }
  }

  test("sendData preserves stallStart across a sub-chunk drip") {
    TestControl.executeEmbed {
      for {
        sq <- streamAndQueue(defaultSettings)
        (stream, _) = sq
        _ <- stream.state.update(_.copy(writeWindow = 0))
        fiber <- stream.sendData(ByteVector.fill(10)(0), endStream = false).start
        _ <- IO.sleep(10.seconds)
        stalled0 <- stream.state.get.map(_.stallStart)
        _ = assert(stalled0.isDefined)
        _ <- stream.receiveWindowUpdate(H2Frame.WindowUpdate(1, 1))
        _ <- IO.sleep(10.seconds)
        stalled1 <- stream.state.get.map(_.stallStart)
        _ <- fiber.cancel
      } yield assertEquals(stalled1, stalled0)
    }
  }

  test("receiveWindowUpdate ignores updates after the local side has closed") {
    for {
      sq <- streamAndQueue(defaultSettings)
      (stream, queue) = sq
      _ <- stream.state.update(
        _.copy(state = H2Stream.StreamState.HalfClosedLocal, writeWindow = 0)
      )
      _ <- stream.receiveWindowUpdate(H2Frame.WindowUpdate(1, 10))
      _ <- stream.receiveWindowUpdate(H2Frame.WindowUpdate(1, 0))
      halfClosed <- stream.state.get
      _ <- stream.state.update(_.copy(state = H2Stream.StreamState.Closed, writeWindow = 0))
      _ <- stream.receiveWindowUpdate(H2Frame.WindowUpdate(1, 10))
      closed <- stream.state.get
      outgoing <- queue.tryTake
    } yield {
      assertEquals(halfClosed.writeWindow, 0)
      assertEquals(closed.writeWindow, 0)
      assertEquals(outgoing, None)
    }
  }

  test("SETTINGS_INITIAL_WINDOW_SIZE does not adjust inactive send windows") {
    for {
      sq <- streamAndQueue(defaultSettings)
      (stream, _) = sq
      _ <- stream.state.update(
        _.copy(state = H2Stream.StreamState.HalfClosedLocal, writeWindow = 10)
      )
      _ <- stream.modifyWriteWindow(5)
      updated <- stream.state.get
    } yield assertEquals(updated.writeWindow, 10)
  }

  test("receiveWindowUpdate restores a negative stream window") {
    for {
      sq <- streamAndQueue(defaultSettings)
      (stream, queue) = sq
      _ <- stream.state.update(_.copy(writeWindow = -10))
      _ <- stream.receiveWindowUpdate(H2Frame.WindowUpdate(1, 5))
      updated <- stream.state.get
      outgoing <- queue.tryTake
    } yield {
      assertEquals(updated.state, H2Stream.StreamState.Open)
      assertEquals(updated.writeWindow, -5)
      assertEquals(outgoing, None)
    }
  }

  test("receiveWindowUpdate does not wrap an overflowing stream window") {
    for {
      sq <- streamAndQueue(defaultSettings)
      (stream, queue) = sq
      _ <- stream.state.update(_.copy(writeWindow = Int.MaxValue))
      _ <- stream.receiveWindowUpdate(H2Frame.WindowUpdate(1, 1))
      updated <- stream.state.get
      outgoing <- queue.take
      reset = outgoing.collectFirst { case frame: H2Frame.RstStream => frame }
    } yield {
      assertEquals(updated.state, H2Stream.StreamState.Closed)
      assertEquals(updated.writeWindow, Int.MaxValue)
      assertEquals(reset.map(_.value.toInt), Some(H2Error.FlowControlError.value))
    }
  }

  test("receiveData counts padding against the stream flow-control window") {
    for {
      sq <- streamAndQueue(defaultSettings)
      (stream, queue) = sq
      _ <- stream.state.update(_.copy(readWindow = 2))
      _ <- stream.receiveData(
        H2Frame.Data(1, ByteVector.empty, Some(ByteVector.fill(2)(0)), endStream = false)
      )
      updated <- stream.state.get
      outgoing <- queue.take
      reset = outgoing.collectFirst { case frame: H2Frame.RstStream => frame }
    } yield {
      assertEquals(updated.state, H2Stream.StreamState.Closed)
      assertEquals(reset.map(_.value.toInt), Some(H2Error.FlowControlError.value))
    }
  }

  test("rstStream is a no-op after the stream is closed") {
    for {
      sq <- streamAndQueue(defaultSettings)
      (stream, queue) = sq
      _ <- stream.state.update(_.copy(state = H2Stream.StreamState.Closed))
      _ <- stream.rstStream(H2Error.StreamClosed)
      outgoing <- queue.tryTake
    } yield assertEquals(outgoing, None)
  }

  test("sendData can end an empty stream after SETTINGS makes its window negative") {
    TestControl.executeEmbed {
      for {
        sq <- streamAndQueue(defaultSettings)
        (stream, queue) = sq
        _ <- stream.state.update(_.copy(writeWindow = -10))
        _ <- stream.sendData(ByteVector.empty, endStream = true).timeout(1.seconds)
        frames <- drainFrames(queue)
        updated <- stream.state.get
      } yield {
        assertEquals(frames, Vector(H2Frame.Data(1, ByteVector.empty, None, endStream = true)))
        assertEquals(updated.writeWindow, -10)
        assertEquals(updated.state, H2Stream.StreamState.HalfClosedLocal)
      }
    }
  }

  test("empty nonterminal DATA waits for credit when the stream window is negative") {
    TestControl.executeEmbed {
      for {
        sq <- streamAndQueue(defaultSettings)
        (stream, queue) = sq
        _ <- stream.state.update(_.copy(writeWindow = -10))
        sentBeforeUpdate <- stream.sendData(ByteVector.empty, endStream = false).background.use {
          completed =>
            for {
              _ <- IO.sleep(1.seconds)
              premature <- queue.tryTake
              _ <- stream.receiveWindowUpdate(H2Frame.WindowUpdate(1, 11))
              _ <- completed.flatMap(_.embedNever)
            } yield premature
        }
        frames <- drainFrames(queue)
      } yield {
        assertEquals(sentBeforeUpdate, None)
        assertEquals(frames, Vector(H2Frame.Data(1, ByteVector.empty, None, endStream = false)))
      }
    }
  }

  List(2L, 3L).foreach { declaredLength =>
    test(s"final DATA checks content-length $declaredLength before closing the local half") {
      for {
        closed <- Ref[IO].of(0)
        sq <- streamAndQueue(defaultSettings, onClosed = closed.update(_ + 1))
        (stream, queue) = sq
        _ <- stream.state.update(
          _.copy(
            state = H2Stream.StreamState.HalfClosedLocal,
            contentLengthCheck = Some((declaredLength, 0L)),
          )
        )
        data = H2Frame.Data(1, ByteVector.fromValidHex("0001"), None, endStream = true)
        _ <- stream.receiveData(data)
        body <- stream.readBody.compile.toVector.attempt
        _ <- stream.receiveData(data)
        _ <- stream.rstStream(H2Error.ProtocolError)
        frames <- drainFrames(queue)
        state <- stream.state.get
        count <- closed.get
      } yield {
        val resets = frames.collect { case rst: H2Frame.RstStream => rst.value.toInt }
        if (declaredLength == 2L) {
          assertEquals(body, Right(Vector[Byte](0, 1)))
          assertEquals(resets, Vector.empty)
        } else {
          assert(body.isLeft, clue(body))
          assertEquals(resets, Vector(H2Error.ProtocolError.value))
        }
        assertEquals(state.state, H2Stream.StreamState.Closed)
        assertEquals(count, 1)
      }
    }
  }

  test("invalid terminal response HEADERS reset and complete waiters before closing") {
    TestControl.executeEmbed {
      for {
        closed <- Ref[IO].of(0)
        sq <- streamAndQueue(
          defaultSettings,
          H2Connection.ConnectionType.Client,
          closed.update(_ + 1),
        )
        (stream, queue) = sq
        _ <- stream.state.update(_.copy(state = H2Stream.StreamState.HalfClosedLocal))
        block <- stream.hpack.encodeHeaders(NonEmptyList.one(("x-not-status", "value", false)))
        _ <- stream.receiveHeaders(
          H2Frame.Headers(1, None, endStream = true, endHeaders = true, block, None),
          Nil,
        )
        response <- stream.getResponse.attempt.timeout(1.seconds)
        body <- stream.readBody.compile.toVector.attempt.timeout(1.seconds)
        _ <- stream.rstStream(H2Error.ProtocolError)
        frames <- drainFrames(queue)
        count <- closed.get
      } yield {
        assert(response.isLeft, clue(response))
        assert(body.isLeft, clue(body))
        assertEquals(
          frames.collect { case rst: H2Frame.RstStream => rst.value.toInt },
          Vector(H2Error.ProtocolError.value),
        )
        assertEquals(count, 1)
      }
    }
  }

  List(false, true).foreach { reset =>
    val action = if (reset) "reset" else "local END_STREAM"
    test(s"remote header decode cannot undo $action while it is suspended") {
      TestControl.executeEmbed {
        for {
          entered <- Deferred[IO, Unit]
          resume <- Deferred[IO, Unit]
          delegate <- Hpack.create[IO](1024)
          gated = new Hpack[IO] {
            def encodeHeaders(headers: NonEmptyList[(String, String, Boolean)]): IO[ByteVector] =
              delegate.encodeHeaders(headers)
            def decodeHeaders(bytes: ByteVector): IO[NonEmptyList[(String, String)]] =
              entered.complete(()).void >> resume.get >> delegate.decodeHeaders(bytes)
            def decodeHeadersAndDiscard(bytes: ByteVector): IO[Unit] =
              delegate.decodeHeadersAndDiscard(bytes)
          }
          closed <- Ref[IO].of(0)
          sq <- streamAndQueue(
            defaultSettings,
            H2Connection.ConnectionType.Client,
            closed.update(_ + 1),
            Some(gated),
          )
          (stream, queue) = sq
          block <- delegate.encodeHeaders(NonEmptyList.one((":status", "200", false)))
          _ <- stream
            .receiveHeaders(
              H2Frame.Headers(1, None, endStream = true, endHeaders = true, block, None),
              Nil,
            )
            .background
            .use { completed =>
              entered.get >>
                (if (reset) stream.rstStream(H2Error.Cancel)
                 else stream.sendData(ByteVector.empty, endStream = true)) >>
                resume.complete(()) >> completed.flatMap(_.embedNever)
            }
          response <- stream.getResponse.attempt.timeout(1.seconds)
          body <- stream.readBody.compile.toVector.attempt.timeout(1.seconds)
          frames <- drainFrames(queue)
          state <- stream.state.get
          count <- closed.get
        } yield {
          assertEquals(state.state, H2Stream.StreamState.Closed)
          assertEquals(count, 1)
          val resets = frames.collect { case rst: H2Frame.RstStream => rst.value.toInt }
          if (reset) {
            assert(response.isLeft, clue(response))
            assert(body.isLeft, clue(body))
            assertEquals(resets, Vector(H2Error.Cancel.value))
          } else {
            assertEquals(response.map(_.status), Right(Status.Ok))
            assertEquals(body, Right(Vector.empty[Byte]))
            assertEquals(resets, Vector.empty)
          }
        }
      }
    }
  }

  test("reset releases DATA waiting on a full body buffer and preserves its failure") {
    TestControl.executeEmbed {
      for {
        closed <- Ref[IO].of(0)
        sq <- streamAndQueue(defaultSettings, onClosed = closed.update(_ + 1))
        (stream, queue) = sq
        buffer <- Channel.bounded[IO, Either[Throwable, ByteVector]](128)
        _ <- buffer.send(Right(ByteVector.empty)).replicateA_(128)
        _ <- stream.state.update(_.copy(readBuffer = buffer, readWindow = 1))
        _ <- stream
          .receiveData(H2Frame.Data(1, ByteVector.fromValidHex("01"), None, endStream = false))
          .background
          .use { completed =>
            // Let receiveData fill its stream window and block before resetting it.
            IO.sleep(1.seconds) >> stream.rstStream(H2Error.Cancel).timeout(1.seconds) >>
              completed.flatMap(_.embedNever).timeout(1.seconds)
          }
        _ <- buffer.closed.timeout(1.seconds)
        body <- stream.readBody.compile.toVector.attempt.timeout(1.seconds)
        frames <- drainFrames(queue)
        count <- closed.get
      } yield {
        assert(body.isLeft, clue(body))
        assertEquals(frames, Vector(H2Error.Cancel.toRst(1)))
        assertEquals(count, 1)
      }
    }
  }

  List(false, true).foreach { requestEnded =>
    test(s"NO_ERROR reset preserves a complete response with request ended $requestEnded") {
      TestControl.executeEmbed {
        for {
          closed <- Ref[IO].of(0)
          sq <- streamAndQueue(
            defaultSettings,
            H2Connection.ConnectionType.Client,
            closed.update(_ + 1),
          )
          (stream, queue) = sq
          _ <- stream.state.update(
            _.copy(state =
              if (requestEnded) H2Stream.StreamState.HalfClosedLocal else H2Stream.StreamState.Open
            )
          )
          block <- stream.hpack.encodeHeaders(
            NonEmptyList.of((":status", "200", false), ("content-length", "2", false))
          )
          _ <- stream.receiveHeaders(
            H2Frame.Headers(1, None, endStream = false, endHeaders = true, block, None),
            Nil,
          )
          _ <- stream.receiveData(
            H2Frame.Data(1, ByteVector.fromValidHex("0001"), None, endStream = true)
          )
          _ <- stream.receiveRstStream(H2Error.NoError.toRst(1))
          response <- stream.getResponse.timeout(1.seconds)
          body <- stream.readBody.compile.toVector.timeout(1.seconds)
          frames <- drainFrames(queue)
          count <- closed.get
        } yield {
          assertEquals(response.status, Status.Ok)
          assertEquals(body, Vector[Byte](0, 1))
          assertEquals(frames, Vector.empty)
          assertEquals(count, 1)
        }
      }
    }
  }

  private def drainFrames(queue: Queue[IO, Chunk[H2Frame]]): IO[Vector[H2Frame]] =
    queue.tryTake.flatMap {
      case Some(chunk) => drainFrames(queue).map(chunk.toVector ++ _)
      case None => IO.pure(Vector.empty)
    }
}
