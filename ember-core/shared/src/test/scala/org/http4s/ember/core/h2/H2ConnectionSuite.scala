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
import org.typelevel.log4cats.noop.NoOpFactory
import scodec.bits.ByteVector

import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

class H2ConnectionSuite extends Http4sSuite {

  private val addr = SocketAddress(ip"127.0.0.1", port"0")

  private def testSocket(
      bytes: ByteVector,
      onWrite: Chunk[Byte] => IO[Unit],
  ): IO[Socket[IO]] =
    Ref[IO].of(bytes).map { ref =>
      new Socket[IO] {
        def read(maxBytes: Int): IO[Option[Chunk[Byte]]] =
          ref.modify { bv =>
            if (bv.isEmpty) (bv, None)
            else {
              val (h, t) = bv.splitAt(maxBytes.toLong)
              (t, Some(Chunk.byteVector(h)))
            }
          }
        // Implement only as necessary...
        def endOfInput: IO[Unit] = ???
        def endOfOutput: IO[Unit] = ???
        def isOpen: IO[Boolean] = ???
        def localAddress: IO[SocketAddress[IpAddress]] = ???
        def peerAddress: GenSocketAddress = ???
        def readN(numBytes: Int): IO[Chunk[Byte]] = ???
        def reads: Stream[IO, Byte] = ???
        def remoteAddress: IO[SocketAddress[IpAddress]] = ???
        def write(bytes: Chunk[Byte]): IO[Unit] = onWrite(bytes)
        def writes: Pipe[IO, Byte, Nothing] = ???
        def address: GenSocketAddress = ???
        def getOption[A](key: SocketOption.Key[A]): IO[Option[A]] = ???
        def setOption[A](key: SocketOption.Key[A], value: A): IO[Unit] = ???
        def supportedOptions: IO[Set[SocketOption.Key[_]]] = ???
      }
    }

  private def readOnlySocket(bytes: ByteVector): IO[Socket[IO]] =
    testSocket(bytes, _ => IO.raiseError(new UnsupportedOperationException("read-only socket")))

  private def mkConnection(
      localSettings: H2Frame.Settings.ConnectionSettings,
      input: ByteVector,
      connectionType: H2Connection.ConnectionType = H2Connection.ConnectionType.Server,
      socketOverride: Option[Socket[IO]] = None,
  ): IO[H2Connection[IO]] =
    for {
      socket <- socketOverride.fold(readOnlySocket(input))(_.pure[IO])
      mapRef <- Ref[IO].of(Map.empty[Int, H2Stream[IO]])
      stateRef <- H2Connection.initState[IO](
        H2Frame.Settings.ConnectionSettings.default,
        H2Frame.Settings.ConnectionSettings.default.initialWindowSize,
        H2Frame.Settings.ConnectionSettings.default.initialWindowSize,
      )
      outgoing <- Queue.unbounded[IO, Chunk[H2Frame]]
      created <- Queue.unbounded[IO, Int]
      closed <- Queue.unbounded[IO, Int]
      hpack <- Hpack.create[IO](
        localSettings.maxHeaderListSize.fold(Int.MaxValue)(_.listSize)
      )
      lock <- Semaphore[IO](1)
      ack <- Deferred[IO, Either[Throwable, H2Frame.Settings.ConnectionSettings]]
      logger <- NoOpFactory[IO].fromClass(classOf[H2ConnectionSuite])
    } yield new H2Connection[IO](
      addr,
      connectionType,
      Duration.Inf,
      Duration.Inf,
      localSettings,
      mapRef,
      stateRef,
      outgoing,
      created,
      closed,
      hpack,
      lock.permit,
      ack,
      ByteVector.empty,
      socket,
      logger,
    )

  private def drainOutgoing(h2: H2Connection[IO]): IO[Vector[H2Frame]] =
    h2.outgoing.tryTake.flatMap {
      case Some(c) => drainOutgoing(h2).map(c.toVector ++ _)
      case None => IO.pure(Vector.empty)
    }

  private def settingsWithMaxHeaderListSize(
      maxHeaderListSize: Int
  ): H2Frame.Settings.ConnectionSettings =
    H2Frame.Settings.ConnectionSettings.default
      .copy(maxHeaderListSize = Some(H2Frame.Settings.SettingsMaxHeaderListSize(maxHeaderListSize)))

  private def frameBytes(frames: H2Frame*): ByteVector =
    frames.foldLeft(ByteVector.empty) { case (acc, frame) =>
      acc ++ H2Frame.toByteVector(frame)
    }

  test("continunation frames within maxHeaderListSize accumulate without GoAway") {
    val headers =
      H2Frame.Headers(1, None, endStream = false, endHeaders = false, ByteVector.fill(40)(0), None)
    val cont = H2Frame.Continuation(1, endHeaders = false, ByteVector.fill(40)(0))
    val input = H2Frame.toByteVector(headers) ++ H2Frame.toByteVector(cont)
    for {
      // input is 40+40, max is 100 ... it fits
      h2 <- mkConnection(settingsWithMaxHeaderListSize(100), input)
      _ <- h2.readLoop
      frames <- drainOutgoing(h2)
      _ = assert(!frames.exists(_.isInstanceOf[H2Frame.GoAway]), clue(frames))
      st <- h2.state.get
      _ = assertEquals(st.headersInProgress.map(_.size), Some(80L))
    } yield ()
  }

  test("continuation frames exceeding maxHeaderListSize trigger GoAway(EnhanceYourCalm)") {
    val headers =
      H2Frame.Headers(1, None, endStream = false, endHeaders = false, ByteVector.fill(60)(0), None)
    val cont = H2Frame.Continuation(1, endHeaders = false, ByteVector.fill(60)(0))
    val input = H2Frame.toByteVector(headers) ++ H2Frame.toByteVector(cont)
    for {
      h2 <- mkConnection(settingsWithMaxHeaderListSize(100), input)
      _ <- h2.readLoop
      frames <- drainOutgoing(h2)
      goAways = frames.collectFirst { case g: H2Frame.GoAway => g }
      _ = assert(goAways.nonEmpty, clue(frames))
      _ = assertEquals(goAways.get.errorCode.toInt, H2Error.EnhanceYourCalm.value)
      closed <- h2.state.get.map(_.closed)
      _ = assert(closed)
    } yield ()
  }

  test("a final continuation exceeding maxHeaderListSize triggers GoAway(EnhanceYourCalm)") {
    val headers =
      H2Frame.Headers(1, None, endStream = false, endHeaders = false, ByteVector.fill(60)(0), None)
    val cont = H2Frame.Continuation(1, endHeaders = true, ByteVector.fill(60)(0))

    for {
      h2 <- mkConnection(settingsWithMaxHeaderListSize(100), frameBytes(headers, cont))
      _ <- h2.readLoop
      frames <- drainOutgoing(h2)
      goAway = frames.collectFirst { case frame: H2Frame.GoAway => frame }
    } yield assertEquals(
      goAway.map(_.errorCode.toInt),
      Some(H2Error.EnhanceYourCalm.value),
      clue(frames),
    )
  }

  test("late WindowUpdate for a closed stream is discarded") {
    val input = frameBytes(H2Frame.WindowUpdate(1, 1), H2Frame.Ping.default)

    for {
      h2 <- mkConnection(H2Frame.Settings.ConnectionSettings.default, input)
      _ <- h2.initiateRemoteStreamById(1)
      _ <- h2.mapRef.update(_ - 1)
      _ <- h2.readLoop
      frames <- drainOutgoing(h2)
    } yield {
      assert(!frames.exists(_.isInstanceOf[H2Frame.GoAway]), clue(frames))
      assert(frames.contains(H2Frame.Ping.ack), clue(frames))
    }
  }

  test("remote stream IDs do not advance the local stream sequence") {
    for {
      h2 <- mkConnection(H2Frame.Settings.ConnectionSettings.default, ByteVector.empty)
      _ <- h2.initiateRemoteStreamById(101)
      first <- h2.initiateLocalStream
      second <- h2.initiateLocalStream
    } yield assertEquals((first.id, second.id), (2, 4))
  }

  test("connection flow control starts at the protocol default window") {
    val settings = H2Frame.Settings.ConnectionSettings.default.copy(
      initialWindowSize = H2Frame.Settings.SettingsInitialWindowSize(1024)
    )

    for {
      h2 <- mkConnection(settings, ByteVector.empty)
      readWindow <- h2.state.get.map(_.readWindow)
    } yield assertEquals(
      readWindow,
      H2Frame.Settings.ConnectionSettings.default.initialWindowSize.windowSize.intValue,
    )
  }

  test("write loop sends an empty DATA frame when the connection window is zero") {
    TestControl.executeEmbed {
      for {
        writes <- Queue.unbounded[IO, Chunk[Byte]]
        socket <- testSocket(ByteVector.empty, writes.offer)
        h2 <- mkConnection(
          H2Frame.Settings.ConnectionSettings.default,
          ByteVector.empty,
          socketOverride = Some(socket),
        )
        _ <- h2.state.update(_.copy(writeWindow = 0))
        writeFiber <- h2.writeLoop.compile.drain.start
        expected = H2Frame.Data(1, ByteVector.empty, None, endStream = true)
        _ <- h2.outgoing.offer(Chunk.singleton(expected))
        bytes <- writes.take.timeout(1.second)
        _ <- writeFiber.cancel
        actual = H2Frame.RawFrame
          .fromByteVector(bytes.toByteVector)
          .flatMap { case (raw, _) => H2Frame.fromRaw(raw).toOption }
      } yield assertEquals(actual, Some(expected))
    }
  }

  test("WindowUpdate for an idle stream still triggers GoAway") {
    val input = frameBytes(H2Frame.WindowUpdate(1, 1))

    for {
      h2 <- mkConnection(H2Frame.Settings.ConnectionSettings.default, input)
      _ <- h2.readLoop
      frames <- drainOutgoing(h2)
    } yield assert(frames.exists(_.isInstanceOf[H2Frame.GoAway]), clue(frames))
  }

  test("zero stream WindowUpdate is a stream error") {
    val input = frameBytes(H2Frame.WindowUpdate(1, 0), H2Frame.Ping.default)

    for {
      h2 <- mkConnection(H2Frame.Settings.ConnectionSettings.default, input)
      stream <- h2.initiateRemoteStreamById(1)
      _ <- stream.state.update(_.copy(state = H2Stream.StreamState.Open))
      _ <- h2.readLoop
      frames <- drainOutgoing(h2)
      rst = frames.collectFirst { case r: H2Frame.RstStream => r }
    } yield {
      assert(!frames.exists(_.isInstanceOf[H2Frame.GoAway]), clue(frames))
      assertEquals(rst.map(_.identifier), Some(1), clue(frames))
      assertEquals(rst.map(_.value.toInt), Some(H2Error.ProtocolError.value), clue(frames))
      assert(frames.contains(H2Frame.Ping.ack), clue(frames))
    }
  }

  test("late DATA for a closed stream is discarded and counts against the connection window") {
    val settings = H2Frame.Settings.ConnectionSettings.default.copy(
      maxFrameSize = H2Frame.Settings.SettingsMaxFrameSize(65535)
    )
    val input = frameBytes(
      H2Frame.Data(
        1,
        ByteVector.fill(32767)(0),
        Some(ByteVector.fill(1)(0)),
        endStream = false,
      ),
      H2Frame.Ping.default,
    )

    for {
      h2 <- mkConnection(settings, input)
      _ <- h2.initiateRemoteStreamById(1)
      _ <- h2.mapRef.update(_ - 1)
      _ <- h2.readLoop
      frames <- drainOutgoing(h2)
      connectionUpdate = frames.collectFirst { case w @ H2Frame.WindowUpdate(0, _) =>
        w
      }
    } yield {
      assert(!frames.exists(_.isInstanceOf[H2Frame.GoAway]), clue(frames))
      assertEquals(connectionUpdate.map(_.windowSizeIncrement), Some(32769), clue(frames))
      assert(frames.contains(H2Frame.Ping.ack), clue(frames))
    }
  }

  test("frame size validation includes DATA padding") {
    val input = frameBytes(
      H2Frame.Data(
        1,
        ByteVector.fill(16380)(0),
        Some(ByteVector.fill(4)(0)),
        endStream = false,
      )
    )

    for {
      h2 <- mkConnection(H2Frame.Settings.ConnectionSettings.default, input)
      _ <- h2.readLoop
      frames <- drainOutgoing(h2)
      goAway = frames.collectFirst { case frame: H2Frame.GoAway => frame }
    } yield assertEquals(
      goAway.map(_.errorCode.toInt),
      Some(H2Error.FrameSizeError.value),
      clue(frames),
    )
  }

  test("an oversized declared frame is rejected before its payload is buffered") {
    val oversizedDataHeader = ByteVector.fromValidHex("004001000000000001")

    for {
      h2 <- mkConnection(H2Frame.Settings.ConnectionSettings.default, oversizedDataHeader)
      _ <- h2.readLoop
      frames <- drainOutgoing(h2)
      goAway = frames.collectFirst { case frame: H2Frame.GoAway => frame }
    } yield assertEquals(
      goAway.map(_.errorCode.toInt),
      Some(H2Error.FrameSizeError.value),
      clue(frames),
    )
  }

  test("an empty late HEADERS block for a closed stream is discarded") {
    val frames = for {
      h2 <- mkConnection(
        H2Frame.Settings.ConnectionSettings.default,
        frameBytes(
          H2Frame.Headers(
            1,
            None,
            endStream = true,
            endHeaders = true,
            ByteVector.empty,
            None,
          ),
          H2Frame.Ping.default,
        ),
      )
      _ <- h2.initiateRemoteStreamById(1)
      _ <- h2.mapRef.update(_ - 1)
      _ <- h2.readLoop
      outgoing <- drainOutgoing(h2)
    } yield outgoing

    frames.map { outgoing =>
      assert(!outgoing.exists(_.isInstanceOf[H2Frame.GoAway]), clue(outgoing))
      assert(outgoing.contains(H2Frame.Ping.ack), clue(outgoing))
    }
  }

  test("HEADERS after the remote side closes update HPACK and reset only the stream") {
    val result = for {
      encoder <- Hpack.create[IO](1024)
      first <- encoder.encodeHeaders(NonEmptyList.one(("x-dynamic", "value", false)))
      second <- encoder.encodeHeaders(NonEmptyList.one(("x-dynamic", "value", false)))
      h2 <- mkConnection(
        H2Frame.Settings.ConnectionSettings.default,
        frameBytes(
          H2Frame.Headers(1, None, endStream = false, endHeaders = true, first, None),
          H2Frame.Headers(1, None, endStream = false, endHeaders = true, second, None),
          H2Frame.Ping.default,
        ),
      )
      stream <- h2.initiateRemoteStreamById(1)
      _ <- stream.state.update(_.copy(state = H2Stream.StreamState.HalfClosedRemote))
      _ <- h2.readLoop
      outgoing <- drainOutgoing(h2)
    } yield outgoing

    result.map { outgoing =>
      val resets = outgoing.collect { case frame: H2Frame.RstStream => frame }
      assert(!outgoing.exists(_.isInstanceOf[H2Frame.GoAway]), clue(outgoing))
      assertEquals(resets.map(_.identifier), Vector(1), clue(outgoing))
      assertEquals(resets.map(_.value.toInt), Vector(H2Error.StreamClosed.value), clue(outgoing))
      assert(outgoing.contains(H2Frame.Ping.ack), clue(outgoing))
    }
  }

  test("late HEADERS continuation for a closed stream is minimally processed") {
    val frames = for {
      encoder <- Hpack.create[IO](1024)
      headerBlock <- encoder.encodeHeaders(
        NonEmptyList.of((":method", "GET", false), (":path", "/", false))
      )
      first = headerBlock.take(1)
      rest = headerBlock.drop(1)
      h2 <- mkConnection(
        H2Frame.Settings.ConnectionSettings.default,
        frameBytes(
          H2Frame.Headers(1, None, endStream = true, endHeaders = false, first, None),
          H2Frame.Continuation(1, endHeaders = true, rest),
          H2Frame.Ping.default,
        ),
      )
      _ <- h2.initiateRemoteStreamById(1)
      _ <- h2.mapRef.update(_ - 1)
      _ <- h2.readLoop
      outgoing <- drainOutgoing(h2)
    } yield outgoing

    frames.map { outgoing =>
      assert(!outgoing.exists(_.isInstanceOf[H2Frame.GoAway]), clue(outgoing))
      assert(outgoing.contains(H2Frame.Ping.ack), clue(outgoing))
    }
  }

  test("PUSH_PROMISE on a closed stream still reserves its promised stream") {
    val result = for {
      encoder <- Hpack.create[IO](1024)
      headerBlock <- encoder.encodeHeaders(
        NonEmptyList.of(
          (":method", "GET", false),
          (":path", "/", false),
          (":scheme", "https", false),
          (":authority", "example.com", false),
        )
      )
      h2 <- mkConnection(
        H2Frame.Settings.ConnectionSettings.default,
        frameBytes(
          H2Frame.PushPromise(1, endHeaders = true, 2, headerBlock, None),
          H2Frame.Ping.default,
        ),
        H2Connection.ConnectionType.Client,
      )
      source <- h2.initiateLocalStream
      _ <- source.state.update(_.copy(state = H2Stream.StreamState.Closed))
      _ <- h2.mapRef.update(_ - source.id)
      _ <- h2.readLoop
      outgoing <- drainOutgoing(h2)
      promised <- h2.mapRef.get.map(_.get(2)).flatMap(_.traverse(_.state.get.map(_.state)))
    } yield (outgoing, promised)

    result.map { case (outgoing, promisedState) =>
      assert(!outgoing.exists(_.isInstanceOf[H2Frame.GoAway]), clue(outgoing))
      assert(outgoing.contains(H2Frame.Ping.ack), clue(outgoing))
      assertEquals(promisedState, Some(H2Stream.StreamState.ReservedRemote))
    }
  }

  test("PUSH_PROMISE continuation uses the originating stream ID") {
    val result = for {
      encoder <- Hpack.create[IO](1024)
      headerBlock <- encoder.encodeHeaders(
        NonEmptyList.of(
          (":method", "GET", false),
          (":path", "/", false),
          (":scheme", "https", false),
          (":authority", "example.com", false),
        )
      )
      first = headerBlock.take(1)
      rest = headerBlock.drop(1)
      h2 <- mkConnection(
        H2Frame.Settings.ConnectionSettings.default,
        frameBytes(
          H2Frame.PushPromise(1, endHeaders = false, 2, first, None),
          H2Frame.Continuation(1, endHeaders = true, rest),
          H2Frame.Ping.default,
        ),
        H2Connection.ConnectionType.Client,
      )
      source <- h2.initiateLocalStream
      _ <- source.state.update(_.copy(state = H2Stream.StreamState.Closed))
      _ <- h2.mapRef.update(_ - source.id)
      _ <- h2.readLoop
      outgoing <- drainOutgoing(h2)
      promised <- h2.mapRef.get.map(_.get(2)).flatMap(_.traverse(_.state.get.map(_.state)))
    } yield (outgoing, promised)

    result.map { case (outgoing, promisedState) =>
      assert(!outgoing.exists(_.isInstanceOf[H2Frame.GoAway]), clue(outgoing))
      assertEquals(promisedState, Some(H2Stream.StreamState.ReservedRemote))
      assert(outgoing.contains(H2Frame.Ping.ack), clue(outgoing))
    }
  }

  test("PUSH_PROMISE on an idle originating stream triggers GoAway") {
    val result = for {
      encoder <- Hpack.create[IO](1024)
      headerBlock <- encoder.encodeHeaders(
        NonEmptyList.of((":method", "GET", false), (":path", "/", false))
      )
      h2 <- mkConnection(
        H2Frame.Settings.ConnectionSettings.default,
        frameBytes(H2Frame.PushPromise(1, endHeaders = true, 2, headerBlock, None)),
        H2Connection.ConnectionType.Client,
      )
      _ <- h2.readLoop
      outgoing <- drainOutgoing(h2)
    } yield outgoing

    result.map(outgoing => assert(outgoing.exists(_.isInstanceOf[H2Frame.GoAway]), clue(outgoing)))
  }

  test("WindowUpdate on a reserved remote stream triggers GoAway") {
    val input = frameBytes(H2Frame.WindowUpdate(2, 1))

    for {
      h2 <- mkConnection(
        H2Frame.Settings.ConnectionSettings.default,
        input,
        H2Connection.ConnectionType.Client,
      )
      stream <- h2.initiateRemoteStreamById(2)
      _ <- stream.state.update(_.copy(state = H2Stream.StreamState.ReservedRemote))
      _ <- h2.readLoop
      frames <- drainOutgoing(h2)
    } yield assert(frames.exists(_.isInstanceOf[H2Frame.GoAway]), clue(frames))
  }

  test("late RST_STREAM for a closed stream is discarded") {
    val input = frameBytes(
      H2Frame.RstStream(1, H2Error.Cancel.value),
      H2Frame.Ping.default,
    )

    for {
      h2 <- mkConnection(H2Frame.Settings.ConnectionSettings.default, input)
      _ <- h2.initiateRemoteStreamById(1)
      _ <- h2.mapRef.update(_ - 1)
      _ <- h2.readLoop
      frames <- drainOutgoing(h2)
    } yield {
      assert(!frames.exists(_.isInstanceOf[H2Frame.GoAway]), clue(frames))
      assert(!frames.exists(_.isInstanceOf[H2Frame.RstStream]), clue(frames))
      assert(frames.contains(H2Frame.Ping.ack), clue(frames))
    }
  }

  test("late frames for a retained closed stream are discarded") {
    val result = for {
      encoder <- Hpack.create[IO](1024)
      headerBlock <- encoder.encodeHeaders(
        NonEmptyList.of((":method", "GET", false), (":path", "/", false))
      )
      h2 <- mkConnection(
        H2Frame.Settings.ConnectionSettings.default,
        frameBytes(
          H2Frame.WindowUpdate(1, 1),
          H2Frame.Data(1, ByteVector.view(Array[Byte](1, 2, 3)), None, endStream = false),
          H2Frame.Headers(1, None, endStream = true, endHeaders = true, headerBlock, None),
          H2Frame.RstStream(1, H2Error.Cancel.value),
          H2Frame.Ping.default,
        ),
      )
      stream <- h2.initiateRemoteStreamById(1)
      _ <- stream.state.update(_.copy(state = H2Stream.StreamState.Closed))
      _ <- h2.readLoop
      outgoing <- drainOutgoing(h2)
    } yield outgoing

    result.map { outgoing =>
      assert(!outgoing.exists(_.isInstanceOf[H2Frame.GoAway]), clue(outgoing))
      assert(!outgoing.exists(_.isInstanceOf[H2Frame.RstStream]), clue(outgoing))
      assert(outgoing.contains(H2Frame.Ping.ack), clue(outgoing))
    }
  }
}
