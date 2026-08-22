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

import cats._
import cats.effect._
import cats.effect.kernel.Outcome
import cats.effect.syntax.all._
import cats.syntax.all._
import com.comcast.ip4s.GenSocketAddress
import fs2._
import fs2.concurrent.Channel
import fs2.io.net.Socket
import org.http4s.ember.core.EmberException
import org.http4s.ember.core.h2.H2Connection.ContinuationProgress
import org.typelevel.log4cats.Logger
import scodec.bits._

import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import H2Frame.Settings.SettingsInitialWindowSize

private[h2] class H2Connection[F[_]](
    address: GenSocketAddress,
    connectionType: H2Connection.ConnectionType,
    receiveHeadersTimeout: Duration,
    idleTimeout: Duration,
    localSettings: H2Frame.Settings.ConnectionSettings,
    val mapRef: Ref[F, Map[Int, H2Stream[F]]],
    val state: Ref[F, H2Connection.State[F]], // odd if client, even if server
    val outgoing: cats.effect.std.Queue[F, Chunk[H2Frame]],
    // val outgoingData: cats.effect.std.Queue[F, Frame.Data], // TODO split data rather than backpressuring frames totally

    val createdStreams: cats.effect.std.Queue[F, Int],
    val closedStreams: cats.effect.std.Queue[F, Int],
    hpack: Hpack[F],
    val streamCreateAndHeaders: Resource[F, Unit],
    val settingsAck: Deferred[F, Either[Throwable, H2Frame.Settings.ConnectionSettings]],
    acc: ByteVector, // Any Bytes Already Read
    socket: Socket[F],
    logger: Logger[F],
)(implicit F: Temporal[F]) {

  private[this] def addrStr = address.toString

  private[this] val maxHeaderBlockSize: Long =
    localSettings.maxHeaderListSize.fold(65536L)(_.listSize.toLong)

  // An unauthenticated peer can open streams without limit.  The 4x
  // gives us slack to reap the closed streams in a graceful fashion,
  // while giving a hard upper bound to protect the server or client
  // in case of abuse.
  private[this] val maxConcurrentRemoteStreams: Long =
    localSettings.maxConcurrentStreams.maxConcurrency.intValue.toLong * 4L

  def initiateLocalStream: F[H2Stream[F]] = for {
    t <- state.modify { s =>
      val highestIsEven = s.highestStream % 2 == 0
      val newHighest = connectionType match {
        case H2Connection.ConnectionType.Server =>
          if (highestIsEven) s.highestStream + 2 else s.highestStream + 1
        case H2Connection.ConnectionType.Client =>
          if (highestIsEven) s.highestStream + 1 else s.highestStream + 2
      }
      (
        s.copy(
          highestStream = newHighest,
          localHighestStream = Math.max(s.localHighestStream, newHighest),
        ),
        (s.remoteSettings, newHighest),
      )
    }
    (settings, id) = t

    writeBlock <- Deferred[F, Either[Throwable, Unit]]
    request <- Deferred[F, Either[Throwable, org.http4s.Request[fs2.Pure]]]
    response <- Deferred[F, Either[Throwable, org.http4s.Response[fs2.Pure]]]
    trailers <- Deferred[F, Either[Throwable, org.http4s.Headers]]
    body <- Channel.bounded[F, Either[Throwable, ByteVector]](128)
    refState <- Ref.of[F, H2Stream.State[F]](
      H2Stream.State(
        H2Stream.StreamState.Idle,
        settings.initialWindowSize.windowSize,
        writeBlock,
        localSettings.initialWindowSize.windowSize,
        request,
        response,
        trailers,
        body,
        None,
        None,
      )
    )
    stream = new H2Stream(
      id,
      idleTimeout,
      localSettings,
      connectionType,
      state.get.map(_.remoteSettings),
      refState,
      hpack,
      outgoing,
      closedStreams.offer(id),
      goAway,
      logger,
    )
    _ <- mapRef.update(m => m + (id -> stream))
  } yield stream

  def initiateRemoteStreamById(id: Int): F[H2Stream[F]] = for {
    openStreams <- mapRef.get.map(_.size)
    _ <-
      if (openStreams >= maxConcurrentRemoteStreams)
        logger.debug(
          s"Open remote streams ($openStreams) at concurrency ceiling: issuing GoAway"
        ) >> goAway(H2Error.EnhanceYourCalm)
      else F.unit
    t <- state.get.map(s => (s.remoteSettings, s.remoteHighestStream))
    (settings, _) = t
    writeBlock <- Deferred[F, Either[Throwable, Unit]]
    request <- Deferred[F, Either[Throwable, org.http4s.Request[fs2.Pure]]]
    response <- Deferred[F, Either[Throwable, org.http4s.Response[fs2.Pure]]]
    trailers <- Deferred[F, Either[Throwable, org.http4s.Headers]]
    body <- Channel.bounded[F, Either[Throwable, ByteVector]](128)
    refState <- Ref.of[F, H2Stream.State[F]](
      H2Stream.State(
        H2Stream.StreamState.Idle,
        settings.initialWindowSize.windowSize,
        writeBlock,
        localSettings.initialWindowSize.windowSize,
        request,
        response,
        trailers,
        body,
        None,
        None,
      )
    )
    stream = new H2Stream(
      id,
      idleTimeout,
      localSettings,
      connectionType,
      state.get.map(_.remoteSettings),
      refState,
      hpack,
      outgoing,
      closedStreams.offer(id),
      goAway,
      logger,
    )
    _ <- mapRef.update(m => m + (id -> stream))
    _ <- state.update(s =>
      s.copy(
        highestStream = Math.max(s.highestStream, id),
        remoteHighestStream = Math.max(s.remoteHighestStream, id),
      )
    )
  } yield stream

  def goAway(error: H2Error): F[Unit] =
    state.get.map(_.remoteHighestStream).flatMap { i =>
      val g = error.toGoAway(i)
      outgoing.offer(Chunk.singleton(g))
    } >>
      H2Connection.KillWithoutMessage().raiseError

  private def isLocalStreamId(id: Int): Boolean =
    connectionType match {
      case H2Connection.ConnectionType.Server => id % 2 == 0
      case H2Connection.ConnectionType.Client => id % 2 != 0
    }

  private def isKnownStreamId(id: Int, s: H2Connection.State[F]): Boolean =
    if (id <= 0) false
    else if (isLocalStreamId(id)) id <= s.localHighestStream
    else id <= s.remoteHighestStream

  private def discardHeaders(
      headers: H2Frame.Headers,
      continuations: List[H2Frame.Continuation],
  ): F[Unit] = {
    val block = headers.headerBlock ++ continuations.foldLeft(ByteVector.empty) {
      case (acc, continuation) => acc ++ continuation.headerBlockFragment
    }
    hpack
      .decodeHeaders(block)
      .onError {
        case e @ EmberException.MessageTooLong(_) =>
          logger.debug(e)("Headers too large") >> goAway(H2Error.EnhanceYourCalm)
        case e =>
          logger.error(e)("Issue in discarded headers") >> goAway(H2Error.CompressionError)
      }
      .void
  }

  private def receiveConnectionData(dataSize: Int): F[Unit] =
    state.get.flatMap { s =>
      val newSize = s.readWindow - dataSize
      if (newSize < 0) goAway(H2Error.FlowControlError)
      else {
        val windowSize = localSettings.initialWindowSize.windowSize
        val needsWindowUpdate = newSize <= (windowSize / 2)
        state.update(_.copy(readWindow = if (needsWindowUpdate) windowSize else newSize)) >>
          (if (needsWindowUpdate)
             outgoing.offer(Chunk.singleton(H2Frame.WindowUpdate(0, windowSize - newSize)))
           else Applicative[F].unit)
      }
    }

  private[this] def writeChunk(chunk: Chunk[H2Frame]): F[Unit] = {
    def go(chunk: Chunk[H2Frame]): F[Unit] = state.get.flatMap { s =>
      val fullDataSize = chunk.foldLeft(0) {
        case (init, H2Frame.Data(_, data, _, _)) => init + data.size.toInt
        case (init, _) => init
      }
      // println(s"Next Write Block Window - data: $fullDataSize window:${s.writeWindow} $s")

      if (fullDataSize <= s.writeWindow && s.writeWindow > 0) {
        val bv = chunk.foldLeft(ByteVector.empty) { case (acc, frame) =>
          acc ++ H2Frame.toByteVector(frame)
        }
        state.update(s => s.copy(writeWindow = s.writeWindow - fullDataSize, stallStart = None)) >>
          socket.write(Chunk.byteVector(bv)) >>
          chunk.traverse_(frame => logger.debug(s"$addrStr Write - $frame"))
      } else {
        val (nonData, after) = chunk.indexWhere(_.isInstanceOf[H2Frame.Data]) match {
          case None => (chunk, Chunk.empty[H2Frame])
          case Some(ix) => chunk.splitAt(ix)
        }

        val bv = nonData.foldLeft(ByteVector.empty) { case (acc, frame) =>
          acc ++ H2Frame.toByteVector(frame)
        }
        socket.write(Chunk.byteVector(bv)) >>
          nonData.traverse_(frame => logger.debug(s"$addrStr Write - $frame")) >>
          Temporal[F].monotonic.flatMap { now =>
            state.update(st => st.copy(stallStart = st.stallStart.orElse(Some(now))))
          } >>
          s.writeBlock.get.rethrow >>
          Temporal[F].monotonic.flatMap { current =>
            state.get.flatMap { st =>
              val elapsed = current - st.stallStart.getOrElse(current)
              if (elapsed >= idleTimeout)
                logger.debug(s"connection stall timeout exceeded ($elapsed)") >>
                  goAway(H2Error.ProtocolError)
              else
                go(after)
            }
          }
      }
    }
    val firstGoAway = chunk.collectFirst { case g: H2Frame.GoAway =>
      mapRef.get.flatMap { m =>
        m.values.toList.traverse_(connection => connection.receiveGoAway(g))
      } >> state.update(s => s.copy(closed = true))
    }
    firstGoAway.getOrElse(F.unit) >> go(chunk)
  }

  def writeLoop: Stream[F, Nothing] =
    Stream
      .fromQueueUnterminated[F, Chunk[H2Frame]](outgoing, Int.MaxValue)
      .foreach(writeChunk)
      .handleErrorWith(ex =>
        Stream.exec(
          logger.debug(ex)("writeLoop terminated") >>
            state.update(_.copy(closed = true))
        )
      )

  // TODO Split Frames between Data and Others Hold Data If we are at cap
  //  Currently will backpressure at the data frame till its cleared

  def readLoop: F[Unit] = {

    def connectionTerminated: String = s"Connection $addrStr readLoop Terminated"
    val readFromSocket: F[Option[Chunk[Byte]]] =
      socket.read(localSettings.initialWindowSize.windowSize)

    def readNextFrame(acc: ByteVector): F[Option[(H2Frame, ByteVector)]] =
      if (acc.isEmpty) {
        readFromSocket.flatMap {
          case Some(chunk) => readNextFrame(chunk.toByteVector)
          case None =>
            logger.debug(s"$connectionTerminated with empty").as(None)
        }
      } else
        H2Frame.RawFrame.fromByteVector(acc) match {
          case Some((raw, leftover)) =>
            H2Frame.fromRaw(raw) match {
              case Right(frame) => F.pure(Some((frame, leftover)))
              case Left(e) =>
                logger.warn(s"$connectionTerminated invalid Raw to Frame $e") >>
                  goAway(e) >> F.pure(None)
            }
          case None =>
            readFromSocket.flatMap {
              case Some(chunk) => readNextFrame(acc ++ chunk.toByteVector)
              case None => logger.debug(s"$connectionTerminated with $acc").as(None)
            }
        }

    def processHeaders(
        headers: H2Frame.Headers,
        continuations: List[H2Frame.Continuation],
        s: H2Connection.State[F],
    ): F[Unit] =
      mapRef.get.map(_.get(headers.identifier)).flatMap {
        case Some(stream) =>
          stream.state.get.flatMap { streamState =>
            if (streamState.state == H2Stream.StreamState.Closed)
              discardHeaders(headers, continuations)
            else stream.receiveHeaders(headers, continuations)
          }
        case None if isKnownStreamId(headers.identifier, s) =>
          discardHeaders(headers, continuations)
        case None =>
          val isValidToCreate = connectionType match {
            case H2Connection.ConnectionType.Server => headers.identifier % 2 != 0
            case H2Connection.ConnectionType.Client => headers.identifier % 2 == 0
          }
          if (
            !isValidToCreate || headers.identifier <= 0 || headers.identifier <= s.remoteHighestStream
          ) {
            logger.warn(
              s"Not Valid Stream to Create ${headers.identifier} - $isValidToCreate, " +
                s"${s.remoteHighestStream} - Protocol Error - Issuing GoAway"
            ) >> goAway(H2Error.ProtocolError)
          } else {
            streamCreateAndHeaders.use(_ =>
              for {
                stream <- initiateRemoteStreamById(headers.identifier)
                _ <- createdStreams.offer(headers.identifier)
                _ <- stream.receiveHeaders(headers, continuations)
              } yield ()
            )
          }
      }

    def processFrame(frame: H2Frame, s: H2Connection.State[F]): F[Unit] = (frame, s) match {
      // Headers and Continuation Frames are Stateful
      // Headers if not closed MUST
      case (
            c @ H2Frame.Continuation(id, true, _),
            H2Connection.State(_, _, _, _, _, _, _, _, Some(headers), None, _),
          ) =>
        if (headers.first.identifier == id) {
          state.update(s => s.copy(headersInProgress = None)) >>
            headers.complete(c).flatMap { case (first, rest) =>
              state.get.flatMap(s => processHeaders(first, rest, s))
            }
        } else {
          logger.warn("Invalid Continuation - Protocol Error - Issuing GoAway") >>
            goAway(H2Error.ProtocolError)
        }
      case (
            c @ H2Frame.Continuation(id, true, _),
            H2Connection.State(_, _, _, _, _, _, _, _, None, Some(pushPromise), _),
          ) =>
        if (pushPromise.first.promisedStreamId == id) {
          state.update(s => s.copy(pushPromiseInProgress = None)) >>
            pushPromise.complete(c).flatMap { case (first, rest) =>
              mapRef.get.map(_.get(id)).flatMap {
                case Some(s) =>
                  s.receivePushPromise(first, rest)
                case None =>
                  streamCreateAndHeaders.use(_ =>
                    for {
                      stream <- initiateRemoteStreamById(id)
                      _ <- createdStreams.offer(id)
                      _ <- stream.receivePushPromise(first, rest)

                    } yield ()
                  )
              }
            }
        } else {
          logger.warn("Invalid Continuation - Protocol Error - Issuing GoAway") >>
            goAway(H2Error.ProtocolError)
        }
      case (
            c @ H2Frame.Continuation(id, false, _),
            H2Connection.State(_, _, _, _, _, _, _, _, None, Some(pushPromise), _),
          ) =>
        if (pushPromise.first.identifier != id) {
          logger.warn("Invalid Continuation - Protocol Error - Issuing GoAway") >>
            goAway(H2Error.ProtocolError)
        } else if (pushPromise.size + c.headerBlockFragment.size > maxHeaderBlockSize) {
          logger.debug("Header block exceeds maxHeaderListSize - Issuing GoAway") >>
            goAway(H2Error.EnhanceYourCalm)
        } else {
          state.update(s => s.copy(pushPromiseInProgress = pushPromise.addContinuation(c).some))
        }

      case (
            c @ H2Frame.Continuation(id, false, _),
            H2Connection.State(_, _, _, _, _, _, _, _, Some(headers), None, _),
          ) =>
        if (headers.first.identifier != id) {
          logger.warn("Invalid Continuation - Protocol Error - Issuing GoAway") >>
            goAway(H2Error.ProtocolError)
        } else if (headers.size + c.headerBlockFragment.size > maxHeaderBlockSize) {
          logger.debug("Header block exceeds maxHeaderListSize - Issuing GoAway") >>
            goAway(H2Error.EnhanceYourCalm)
        } else {
          state.update(s => s.copy(headersInProgress = headers.addContinuation(c).some))
        }
      case (f, H2Connection.State(_, _, _, _, _, _, _, _, Some(_), None, _)) =>
        // Only Continuation Frames Are Valid While there is a value
        logger.warn(
          s"Continuation for headers in process, retrieved unexpected frame $f -  Protocol Error - Issuing GoAway"
        ) >>
          goAway(H2Error.ProtocolError)
      case (f, H2Connection.State(_, _, _, _, _, _, _, _, None, Some(_), _)) =>
        // Only Continuation Frames Are Valid While there is a value
        logger.warn(
          s"Continuation for push promise in process, retrieved unexpected frame $f -  Protocol Error - Issuing GoAway"
        ) >>
          goAway(H2Error.ProtocolError)

      case (h @ H2Frame.Headers(i, sd, _, true, headerBlock, _), s) =>
        val size = headerBlock.size.toInt
        if (size > s.remoteSettings.maxFrameSize.frameSize) {
          logger.warn("Header Size too large for frame size - FrameSizeError - Issuing GoAway") >>
            goAway(H2Error.FrameSizeError)
        } else if (sd.exists(s => s.dependency == i)) {
          goAway(H2Error.ProtocolError)
        } else {
          processHeaders(h, List.empty, s)
        }
      case (h @ H2Frame.Headers(i, sd, _, false, headerBlock, _), s) =>
        val size = headerBlock.size.toInt
        if (size > s.remoteSettings.maxFrameSize.frameSize) goAway(H2Error.FrameSizeError)
        else if (sd.exists(s => s.dependency == i)) goAway(H2Error.ProtocolError)
        else if (headerBlock.size > maxHeaderBlockSize)
          logger.debug("Header block exceeds maxHeaderListSize - Issuing GoAway") >>
            goAway(H2Error.EnhanceYourCalm)
        else {
          ContinuationProgress
            .start(h, headerBlock.size, receiveHeadersTimeout, goAway(H2Error.EnhanceYourCalm))
            .flatMap(headers => state.update(s => s.copy(headersInProgress = Some(headers))))
        }
      case (h @ H2Frame.PushPromise(_, true, i, headerBlock, _), s) =>
        val size = headerBlock.size.toInt
        if (connectionType == H2Connection.ConnectionType.Server) {
          logger.warn(
            "Encountered Push Promise Frame a a Server - Protocol Error - Issuing GoAway"
          ) >>
            goAway(H2Error.ProtocolError)
        } else if (size > s.remoteSettings.maxFrameSize.frameSize) {
          logger.warn("Header Size too large for frame size - FrameSizeError - Issuing GoAway") >>
            goAway(H2Error.FrameSizeError)
        } else {
          mapRef.get.map(_.get(i)).flatMap {
            case Some(s) =>
              s.receivePushPromise(h, List.empty)
            case None =>
              val isValidToCreate = i % 2 == 0
              if (!isValidToCreate || i <= 0 || i <= s.remoteHighestStream) {
                logger.warn(
                  s"Not Valid Stream to Create $i - $isValidToCreate, ${s.remoteHighestStream} - Protocol Error - Issuing GoAway"
                )
                goAway(H2Error.ProtocolError)
              } else {
                streamCreateAndHeaders.use(_ =>
                  for {
                    stream <- initiateRemoteStreamById(i)
                    _ <- createdStreams.offer(i)
                    _ <- stream.receivePushPromise(h, List.empty)
                  } yield ()
                )
              }
          }
        }
      case (h @ H2Frame.PushPromise(_, false, _, headerBlock, _), s) =>
        val size = headerBlock.size.toInt
        if (size > s.remoteSettings.maxFrameSize.frameSize) goAway(H2Error.FrameSizeError)
        else if (headerBlock.size > maxHeaderBlockSize)
          logger.debug("Header block exceeds maxHeaderListSize - Issuing GoAway") >>
            goAway(H2Error.EnhanceYourCalm)
        else {
          ContinuationProgress
            .start(h, headerBlock.size, receiveHeadersTimeout, goAway(H2Error.EnhanceYourCalm))
            .flatMap(pushPromise =>
              state.update(s => s.copy(pushPromiseInProgress = Some(pushPromise)))
            )
        }

      case (H2Frame.Continuation(_, _, _), _) =>
        goAway(H2Error.ProtocolError)

      case (settings @ H2Frame.Settings(0, false, _), _) =>
        for {
          newWriteBlock <- Deferred[F, Either[Throwable, Unit]]
          t <- state.modify { s =>
            val newSettings = H2Frame.Settings.updateSettings(settings, s.remoteSettings)
            val differenceInWindow =
              newSettings.initialWindowSize.windowSize - s.remoteSettings.initialWindowSize.windowSize
            (
              s.copy(
                remoteSettings = newSettings,
                writeWindow = s.writeWindow,
                writeBlock = newWriteBlock,
              ),
              (newSettings, differenceInWindow, s.writeBlock),
            )
          }
          (settings, difference, oldWriteBlock) = t
          _ <- oldWriteBlock.complete(Either.unit)
          _ <- mapRef.get.flatMap { map =>
            map.toList.traverse { case (_, stream) =>
              stream.modifyWriteWindow(difference)
            }
          }
          _ <- outgoing.offer(Chunk.singleton(H2Frame.Settings.Ack))
          _ <- settingsAck.complete(Either.right(settings)).void

        } yield ()
      case (H2Frame.Settings(0, true, _), _) => Applicative[F].unit
      case (H2Frame.Settings(_, _, _), _) =>
        logger.warn("Received Settings Not Oriented at Identifier 0 - Issuing goAway") >>
          goAway(H2Error.ProtocolError)
      case (g @ H2Frame.GoAway(0, _, _, _), _) =>
        mapRef.get.flatMap { m =>
          m.values.toList.traverse_(connection => connection.receiveGoAway(g))
        } >> outgoing.offer(Chunk.singleton(H2Frame.Ping.ack))
      case (_: H2Frame.GoAway, _) =>
        goAway(H2Error.ProtocolError)
      case (H2Frame.Ping(0, false, bv), _) =>
        outgoing.offer(Chunk.singleton(H2Frame.Ping.ack.copy(data = bv)))
      case (H2Frame.Ping(0, true, _), _) => Applicative[F].unit
      case (H2Frame.Ping(_, _, _), _) =>
        goAway(H2Error.ProtocolError)

      case (H2Frame.WindowUpdate(0, 0), _) =>
        logger.warn(
          "Encountered 0 Sized Connection Window Update - Protocol Error - Issuing GoAway"
        ) >>
          goAway(H2Error.ProtocolError)
      case (H2Frame.WindowUpdate(i, 0), s) =>
        mapRef.get.map(_.get(i)).flatMap {
          case Some(stream) =>
            stream.state.get.flatMap { streamState =>
              streamState.state match {
                case H2Stream.StreamState.Closed => Applicative[F].unit
                case H2Stream.StreamState.Idle => goAway(H2Error.ProtocolError)
                case _ => stream.rstStream(H2Error.ProtocolError)
              }
            }
          case None if isKnownStreamId(i, s) =>
            logger.debug(s"Discarding zero WindowUpdate for closed stream $i")
          case None =>
            logger.warn(
              s"Encountered zero WindowUpdate for idle stream $i - Protocol Error - Issuing GoAway"
            ) >> goAway(H2Error.ProtocolError)
        }
      case (w @ H2Frame.WindowUpdate(i, size), s) =>
        i match {
          case 0 =>
            for {
              newWriteBlock <- Deferred[F, Either[Throwable, Unit]]
              t <- state.modify { s =>
                val newSize = s.writeWindow.toLong + size.toLong
                val sizeValid =
                  s.writeWindow >= 0 && newSize <= Int.MaxValue
                (
                  s.copy(
                    writeBlock = newWriteBlock,
                    writeWindow = if (sizeValid) newSize.toInt else s.writeWindow,
                  ),
                  (s.writeBlock, sizeValid),
                )
              }
              (oldWriteBlock, valid) = t
              _ <- oldWriteBlock.complete(Either.unit)
              _ <- {
                if (!valid) goAway(H2Error.FlowControlError)
                else Applicative[F].unit
              }
            } yield ()
          case otherwise =>
            mapRef.get.map(_.get(otherwise)).flatMap {
              case Some(stream) =>
                stream.state.get.flatMap { streamState =>
                  streamState.state match {
                    case H2Stream.StreamState.Idle => goAway(H2Error.ProtocolError)
                    case _ => stream.receiveWindowUpdate(w)
                  }
                }
              case None if isKnownStreamId(otherwise, s) =>
                logger.debug(s"Discarding WindowUpdate for closed stream $w, $otherwise")
              case None =>
                logger.warn(s"Received WindowUpdate for idle stream - $w, $i") >>
                  goAway(H2Error.ProtocolError)
            }
        }

      case (d @ H2Frame.Data(i, data, _, _), _) =>
        val size = data.size.toInt
        if (size > localSettings.maxFrameSize.frameSize) {
          logger.warn(
            "Receive Data Size Larger than Allowed Frame Size - Frame Size Error - Issuing GoAway"
          ) >>
            goAway(H2Error.FrameSizeError)
        } else {
          mapRef.get.map(_.get(i)).flatMap {
            case Some(stream) =>
              stream.state.get.flatMap { streamState =>
                streamState.state match {
                  case H2Stream.StreamState.Idle | H2Stream.StreamState.ReservedLocal |
                      H2Stream.StreamState.ReservedRemote =>
                    goAway(H2Error.ProtocolError)
                  case H2Stream.StreamState.Closed =>
                    receiveConnectionData(size)
                  case _ =>
                    receiveConnectionData(size) >> stream.receiveData(d)
                }
              }
            case None if isKnownStreamId(i, s) =>
              receiveConnectionData(size)
            case None =>
              logger.warn(
                s"Received Data Frame for idle stream $i - Protocol Error - Issuing GoAway"
              ) >>
                goAway(H2Error.ProtocolError)
          }
        }

      case (rst @ H2Frame.RstStream(i, _), s) =>
        mapRef.get.map(_.get(i)).flatMap {
          case Some(s) =>
            s.receiveRstStream(rst)
          case None if isKnownStreamId(i, s) =>
            logger.debug(s"Discarding RstStream for closed stream $i")
          case None =>
            logger.warn(
              s"Received RstStream for idle stream $i - Protocol Error - Issuing GoAway"
            ) >>
              goAway(H2Error.ProtocolError)
        }
      case (H2Frame.Priority(i, _, i2, _), _) =>
        if (i == i2) goAway(H2Error.ProtocolError) // Can't depend on yourself
        else Applicative[F].unit // We Do Nothing with these presently
      case (H2Frame.Unknown(_), _) => Applicative[F].unit // Ignore Unknown Frames
    }

    def readLoopAux(acc: ByteVector): F[Unit] =
      readNextFrame(acc).flatMap {
        case Some((frame, nacc)) =>
          logger.debug(s"$addrStr Read - $frame") >>
            state.get.flatMap(processFrame(frame, _)) >>
            readLoopAux(nacc)
        case None => F.unit
      }

    readLoopAux(acc)
      .recoverWith { case H2Connection.KillWithoutMessage() =>
        logger.debug(s"ReadLoop has received that is should kill")
      }
      .guaranteeCase {
        case Outcome.Errored(e) =>
          logger.error(e)(s"ReadLoop has errored") >>
            goAway(H2Error.InternalError).attempt.void >>
            state.update(s => s.copy(closed = true))
        case _ =>
          state.update(s => s.copy(closed = true))
      }
  }

}

private[h2] object H2Connection {
  final case class State[F[_]](
      remoteSettings: H2Frame.Settings.ConnectionSettings,
      writeWindow: Int,
      writeBlock: Deferred[F, Either[Throwable, Unit]],
      readWindow: Int,
      highestStream: Int,
      localHighestStream: Int,
      remoteHighestStream: Int,
      closed: Boolean,
      headersInProgress: Option[ContinuationProgress[F, H2Frame.Headers]],
      pushPromiseInProgress: Option[ContinuationProgress[F, H2Frame.PushPromise]],
      stallStart: Option[FiniteDuration],
  )

  final class ContinuationProgress[F[_]: Applicative, A](
      val first: A,
      rest: List[H2Frame.Continuation],
      val size: Long,
      timeout: Fiber[F, Throwable, Unit],
  ) {
    def addContinuation(next: H2Frame.Continuation): ContinuationProgress[F, A] =
      new ContinuationProgress(first, next :: rest, size + next.headerBlockFragment.size, timeout)

    def complete(last: H2Frame.Continuation): F[(A, List[H2Frame.Continuation])] =
      timeout.cancel *> Applicative[F].pure(first -> (last :: rest).reverse)
  }

  object ContinuationProgress {
    def start[F[_]: Temporal, A](
        first: A,
        initialSize: Long,
        timeout: Duration,
        cancel: F[Unit],
    ): F[ContinuationProgress[F, A]] =
      (Temporal[F].sleep(timeout) >> cancel.attempt.void).start
        .map(new ContinuationProgress(first, List.empty, initialSize, _))

  }

  def initState[F[_]](
      remoteSettings: H2Frame.Settings.ConnectionSettings,
      writeWindow: SettingsInitialWindowSize,
      readWindow: SettingsInitialWindowSize,
  )(implicit F: Async[F]): F[Ref[F, State[F]]] =
    Deferred[F, Either[Throwable, Unit]].flatMap { writeBlock =>
      val state = H2Connection.State(
        remoteSettings,
        writeWindow.windowSize,
        writeBlock,
        readWindow.windowSize,
        highestStream = 0,
        localHighestStream = 0,
        remoteHighestStream = 0,
        closed = false,
        headersInProgress = None,
        pushPromiseInProgress = None,
        stallStart = None,
      )
      F.ref(state)
    }

  final case class KillWithoutMessage()
      extends RuntimeException
      with scala.util.control.NoStackTrace

  sealed trait ConnectionType
  object ConnectionType {
    case object Server extends ConnectionType
    case object Client extends ConnectionType
  }

}
