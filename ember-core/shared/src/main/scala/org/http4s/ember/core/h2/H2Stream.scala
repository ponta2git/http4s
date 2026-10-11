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
import cats.data._
import cats.effect._
import cats.syntax.all._
import fs2._
import fs2.concurrent.Channel
import org.http4s.Header
import org.http4s.Headers
import org.http4s.Message
import org.http4s.ember.core.EmberException
import org.typelevel.log4cats.Logger
import scodec.bits._

import java.util.concurrent.CancellationException
import scala.annotation.nowarn
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

// Will eventually hold client/server through single interface matching that of the designed paradigm
// in StreamState
@nowarn("msg=implicit numeric widening")
private[h2] class H2Stream[F[_]: Temporal](
    val id: Int,
    sendTimeout: Duration,
    localSettings: H2Frame.Settings.ConnectionSettings,
    connectionType: H2Connection.ConnectionType,
    val remoteSettings: F[H2Frame.Settings.ConnectionSettings],
    val state: Ref[F, H2Stream.State[F]],
    val hpack: Hpack[F],
    val enqueue: cats.effect.std.Queue[F, Chunk[H2Frame]],
    val onClosed: F[Unit],
    val goAway: H2Error => F[Unit],
    private[this] val logger: Logger[F],
    control: Option[cats.effect.std.Queue[F, Chunk[H2Frame]]] = None,
) {
  import H2Stream.{DataReceiveResult, StreamState, WindowUpdateResult}

  private def withHpackErrorHandling[A](fa: F[A]): F[A] =
    fa.onError {
      case e @ EmberException.MessageTooLong(_) =>
        logger.debug(e)("Headers too large") >> goAway(H2Error.EnhanceYourCalm)
      case e =>
        logger.error(e)("Issue in headers") >> goAway(H2Error.CompressionError)
    }

  def sendPushPromise(originating: Int, headers: NonEmptyList[(String, String, Boolean)]): F[Unit] =
    if (!PseudoHeaders.validOutgoing(headers, request = true))
      new IllegalArgumentException("Invalid HTTP/2 push fields").raiseError[F, Unit]
    else
      connectionType match {
        case H2Connection.ConnectionType.Server =>
          state.get.flatMap { s =>
            s.state match {
              case StreamState.Idle =>
                for {
                  _ <- hpack.encodeHeadersWith(headers) { h =>
                    remoteSettings.flatMap { settings =>
                      val size = settings.maxFrameSize.frameSize
                      val first = h.take(size - 4L)
                      val rest = h.drop(size - 4L)
                      val frame = H2Frame.PushPromise(originating, rest.isEmpty, id, first, None)
                      Temporal[F].uncancelable { poll =>
                        state.update(s => s.copy(state = StreamState.ReservedLocal)) >>
                          poll(enqueue.offer(headerFrames(frame, originating, rest, size)))
                      }
                    }
                  }
                } yield ()
              case _ =>
                new IllegalStateException(
                  "Push Promises are only allowed on an idle Stream"
                ).raiseError
            }
          }
        case H2Connection.ConnectionType.Client =>
          new IllegalStateException("Clients Are Not Allowed To Send PushPromises").raiseError
      }

  /** Send [[Message]] in chunks according to remote receiver's max frame size.
    * On empty [[Message]], send an empty DATA frame if there are no trailer headers.
    *
    * @param mess the [[Message]] to send
    */
  def sendMessageBody(mess: Message[F]): F[Unit] =
    sendMessageBody(mess, Applicative[F].unit)

  def sendMessageBody(mess: Message[F], beforeBody: F[Unit]): F[Unit] = {
    val noTrailers = !mess.attributes.contains(Message.Keys.TrailerHeaders[F])
    val maxFrameSize = remoteSettings.map(_.maxFrameSize.frameSize)
    maxFrameSize.flatMap { maxFrameSize =>
      val sendBody =
        mess.body
          .chunkLimit(maxFrameSize)
          .foreach(c => sendData(c.toByteVector, endStream = false))
          .translate(new (F ~> F) {
            def apply[A](fa: F[A]): F[A] =
              Temporal[F].uncancelable { poll =>
                // Register ownership inside the first actual body effect. fs2's resource
                // acquisition mask protects leading finalizers; ordinary body and write
                // effects remain cancelable, including those fs2 evaluates in another fiber.
                beforeBody >> poll(fa)
              }
          })
          .compile
          .drain >> sendData(ByteVector.empty, endStream = true).whenA(noTrailers)
      sendBody.onError { case _ =>
        rstStream(H2Error.InternalError)
      }
    }
  }

  def sendTrailerHeaders(mess: Message[F]): F[Unit] =
    mess.attributes.lookup(Message.Keys.TrailerHeaders[F]) match {
      case None => Applicative[F].unit
      case Some(fhs) =>
        fhs.flatMap { hs =>
          if (
            !PseudoHeaders.validTrailers(
              hs.headers.map(h => h.name.toString.toLowerCase(java.util.Locale.ROOT) -> h.value)
            )
          )
            new IllegalArgumentException("Invalid HTTP/2 trailers").raiseError[F, Unit]
          else
            hs.headers
              .map(a =>
                (
                  a.name.toString.toLowerCase(java.util.Locale.ROOT),
                  a.value,
                  Headers.SensitiveHeaders(a.name),
                )
              )
              .toNel
              .fold(sendData(ByteVector.empty, endStream = true))(sendHeaders(_, endStream = true))
        }
    }

  private def headerFrames(
      first: H2Frame,
      streamId: Int,
      rest: ByteVector,
      size: Int,
  ): Chunk[H2Frame] = {
    val fragments = rest.grouped(size.toLong).toVector
    Chunk.from(Vector(first) ++ fragments.zipWithIndex.map { case (bytes, index) =>
      H2Frame.Continuation(streamId, index == fragments.size - 1, bytes)
    })
  }

  def sendHeaders(headers: NonEmptyList[(String, String, Boolean)], endStream: Boolean): F[Unit] =
    if (!PseudoHeaders.validOutgoing(headers, connectionType == H2Connection.ConnectionType.Client))
      new IllegalArgumentException("Invalid HTTP/2 fields").raiseError[F, Unit]
    else
      state.get.flatMap { s =>
        s.state match {
          case StreamState.Idle | StreamState.HalfClosedRemote | StreamState.Open |
              StreamState.ReservedLocal =>
            hpack.encodeHeadersWith(headers) { bv =>
              remoteSettings.flatMap { settings =>
                val size = settings.maxFrameSize.frameSize
                val f =
                  H2Frame.Headers(id, None, endStream, bv.size <= size, bv.take(size.toLong), None)
                Temporal[F].uncancelable { poll =>
                  // Save the request method for HEAD/CONNECT response framing.
                  s.request
                    .complete(
                      PseudoHeaders
                        .headersToRequestNoBody(headers.map(h => (h._1, h._2)))
                        .toRight(new IllegalArgumentException("Invalid HTTP/2 request"))
                    )
                    .void
                    .whenA(
                      connectionType == H2Connection.ConnectionType.Client && s.state == StreamState.Idle
                    ) >>
                    state
                      .modify { b =>
                        val newState: StreamState = (b.state, endStream) match {
                          case (StreamState.Idle, false) => StreamState.Open
                          case (StreamState.Idle, true) => StreamState.HalfClosedLocal
                          case (StreamState.HalfClosedRemote, false) => StreamState.HalfClosedRemote
                          case (StreamState.HalfClosedRemote, true) => StreamState.Closed
                          case (StreamState.Open, false) => StreamState.Open
                          case (StreamState.Open, true) => StreamState.HalfClosedLocal
                          case (StreamState.ReservedLocal, true) => StreamState.Closed
                          case (StreamState.ReservedLocal, false) => StreamState.HalfClosedRemote
                          case (st, _) => st // Hopefully Impossible
                        }
                        (b.copy(state = newState), newState)
                      }
                      .flatMap { state =>
                        poll(enqueue.offer(headerFrames(f, id, bv.drop(size.toLong), size))) >>
                          onClosed.whenA(state == StreamState.Closed)
                      }
                }
              }
            }
          case _ => new IllegalStateException("Stream Was Closed").raiseError
        }
      }

  def sendData(bv: ByteVector, endStream: Boolean): F[Unit] =
    remoteSettings.flatMap { settings =>
      val size = settings.maxFrameSize.frameSize.toLong
      if (bv.size > size)
        sendData(bv.take(size), endStream = false) >> sendData(bv.drop(size), endStream)
      else sendDataWithinWindow(bv, endStream)
    }

  private def sendDataWithinWindow(bv: ByteVector, endStream: Boolean): F[Unit] =
    Temporal[F].uncancelable { poll =>
      // Reserve against the current window atomically with SETTINGS/WINDOW_UPDATE.
      state
        .modify { s =>
          s.state match {
            case StreamState.Open | StreamState.HalfClosedRemote =>
              if (
                (bv.isEmpty && endStream) || bv.size <= s.writeWindow.toLong || s.writeWindow > 0
              ) {
                val count = math.min(bv.size, math.max(0, s.writeWindow).toLong).toInt
                (
                  s.copy(
                    writeWindow = s.writeWindow - count,
                    stallStart = if (count.toLong == bv.size) None else s.stallStart,
                  ),
                  (s, count),
                )
              } else (s, (s, -1))
            case _ => (s, (s, -2))
          }
        }
        .flatMap {
          case (_, -2) => new IllegalStateException("Stream Was Closed").raiseError[F, Unit]
          case (s, -1) =>
            Temporal[F].monotonic
              .flatMap { now =>
                state.modify { current =>
                  val start = current.stallStart.getOrElse(now)
                  (current.copy(stallStart = Some(start)), now - start)
                }
              }
              .flatMap { elapsed =>
                val remaining = sendTimeout - elapsed
                if (remaining <= Duration.Zero) rstStream(H2Error.ProtocolError)
                else
                  poll(Temporal[F].timeout(s.writeBlock.get.rethrow, remaining)).attempt.flatMap {
                    case Right(_) => poll(sendData(bv, endStream))
                    case Left(_: java.util.concurrent.TimeoutException) =>
                      rstStream(H2Error.ProtocolError)
                    case Left(error) => error.raiseError[F, Unit]
                  }
              }
          case (_, count) =>
            val (head, tail) = bv.splitAt(count.toLong)
            val terminal = tail.isEmpty && endStream
            poll(enqueue.offer(Chunk.singleton(H2Frame.Data(id, head, None, terminal)))) >>
              (if (terminal)
                 state
                   .modify { s =>
                     val next = s.state match {
                       case StreamState.Open => StreamState.HalfClosedLocal
                       case StreamState.HalfClosedRemote => StreamState.Closed
                       case other => other
                     }
                     (
                       s.copy(state = next),
                       next == StreamState.Closed && s.state != StreamState.Closed,
                     )
                   }
                   .flatMap(onClosed.whenA)
               else if (tail.nonEmpty) poll(sendData(tail, endStream))
               else Applicative[F].unit)
        }
    }

  def receiveHeaders(
      headers: H2Frame.Headers,
      continuations: List[H2Frame.Continuation],
  ): F[Unit] = {

    def initialMessage(length: Option[Long])(publish: F[Unit]): Option[F[Unit]] =
      cats
        .Alternative[Option]
        .guard(!headers.endStream || length.forall(_ == 0))
        .as(
          state.update(s => s.copy(contentLengthCheck = length.map(_ -> 0L))) >> publish
        )

    val block = headers.headerBlock ++ continuations.foldLeft(ByteVector.empty) {
      case (acc, cont) => acc ++ cont.headerBlockFragment
    }

    state.get.flatMap { s =>
      def attribute(mess: Message[Pure]): mess.Self = {
        val iMess = mess.withAttribute(H2Keys.StreamIdentifier, id)
        val trailerF = s.trailers.get.rethrow
        iMess.withAttribute(org.http4s.Message.Keys.TrailerHeaders[F], trailerF)
      }

      s.state match {
        case StreamState.Idle if connectionType == H2Connection.ConnectionType.Client =>
          goAway(H2Error.ProtocolError)
        case StreamState.Open | StreamState.HalfClosedLocal | StreamState.Idle |
            StreamState.ReservedRemote =>
          withHpackErrorHandling(hpack.decodeHeaders(block)).flatMap { h =>
            // Parse before closing the stream. A malformed final field section still needs
            // to reset it and fail any request/response or body reader waiting on this stream.
            def trailers: Option[F[Unit]] = cats
              .Alternative[Option]
              .guard(
                headers.endStream && PseudoHeaders.validTrailers(h) &&
                  s.contentLengthCheck.forall { case (expected, actual) => expected == actual }
              )
              .as(s.trailWith(h).void)

            val prepared: F[Option[F[Unit]]] = connectionType match {
              case H2Connection.ConnectionType.Client =>
                s.response.tryGet.flatMap {
                  case None =>
                    s.request.tryGet.map { request =>
                      PseudoHeaders.headersToResponseNoBody(h).flatMap { resp =>
                        if (resp.status.code < 200)
                          cats.Alternative[Option].guard(!headers.endStream).as(Applicative[F].unit)
                        else {
                          val method = request.flatMap(_.toOption).map(_.method)
                          val noContent = method.contains(org.http4s.Method.HEAD) ||
                            resp.status.code == 204 || resp.status.code == 304
                          val tunnel =
                            method.contains(org.http4s.Method.CONNECT) && resp.status.isSuccess
                          val length =
                            if (noContent) Some(0L) else if (tunnel) None else resp.contentLength
                          initialMessage(length)(
                            s.response.complete(Either.right(attribute(resp))).void >>
                              s.trailWith(List.empty).void.whenA(headers.endStream)
                          )
                        }
                      }
                    }
                  case Some(_) => trailers.pure[F]
                }
              case H2Connection.ConnectionType.Server =>
                s.request.tryGet.map {
                  case None =>
                    PseudoHeaders.headersToRequestNoBody(h).flatMap { req =>
                      initialMessage(req.contentLength)(
                        s.request.complete(Either.right(attribute(req))).void >>
                          s.trailWith(List.empty).void.whenA(headers.endStream)
                      )
                    }
                  case Some(_) => trailers
                }
            }

            prepared.flatMap {
              case None =>
                logger.error("Headers Unable to be parsed") >> rstStream(H2Error.ProtocolError)
              case Some(publish) =>
                state
                  .modify[Either[StreamState, StreamState]] { current =>
                    current.state match {
                      case StreamState.Open | StreamState.HalfClosedLocal | StreamState.Idle |
                          StreamState.ReservedRemote =>
                        val next = (current.state, headers.endStream) match {
                          case (StreamState.Open | StreamState.Idle, true) =>
                            StreamState.HalfClosedRemote
                          case (StreamState.HalfClosedLocal | StreamState.ReservedRemote, true) =>
                            StreamState.Closed
                          case (StreamState.Idle, false) => StreamState.Open
                          case (StreamState.ReservedRemote, false) => StreamState.HalfClosedLocal
                          case (other, _) => other
                        }
                        (current.copy(state = next), Right(next))
                      case other => (current, Left(other))
                    }
                  }
                  .flatMap {
                    case Left(StreamState.Closed) => Applicative[F].unit
                    case Left(StreamState.HalfClosedRemote) => rstStream(H2Error.StreamClosed)
                    case Left(_) => goAway(H2Error.ProtocolError)
                    case Right(next) =>
                      publish >> s.readBuffer.close.void.whenA(headers.endStream) >>
                        onClosed.whenA(next == StreamState.Closed)
                  }
            }
          }
        case StreamState.HalfClosedRemote =>
          withHpackErrorHandling(hpack.decodeHeadersAndDiscard(block)) >>
            rstStream(H2Error.StreamClosed)
        case StreamState.Closed =>
          withHpackErrorHandling(hpack.decodeHeadersAndDiscard(block))
        case StreamState.ReservedLocal =>
          goAway(H2Error.ProtocolError)
      }
    }
  }

  def receivePushPromise(
      headers: H2Frame.PushPromise,
      continuations: List[H2Frame.Continuation],
  ): F[Unit] = state.get.flatMap { s =>
    connectionType match {
      case H2Connection.ConnectionType.Client =>
        s.state match {
          case StreamState.Idle =>
            val block = headers.headerBlock ++ continuations.foldLeft(ByteVector.empty) {
              case (acc, cont) => acc ++ cont.headerBlockFragment
            }
            for {
              h <- withHpackErrorHandling(hpack.decodeHeaders(block))
              _ <- state.update(s => s.copy(state = StreamState.ReservedRemote))
              _ <- PseudoHeaders.headersToRequestNoBody(h) match {
                case Some(req)
                    if (req.method == org.http4s.Method.GET || req.method == org.http4s.Method.HEAD) &&
                      req.contentLength.forall(_ == 0) =>
                  val attributed =
                    req
                      .withAttribute(H2Keys.StreamIdentifier, id)
                      .withAttribute(H2Keys.PushPromiseInitialStreamIdentifier, headers.identifier)
                  s.request.complete(Either.right(attributed)).void
                case _ => rstStream(H2Error.ProtocolError)
              }
            } yield ()

          case _ => goAway(H2Error.ProtocolError)
        }
      case H2Connection.ConnectionType.Server =>
        goAway(H2Error.ProtocolError)
    }
  }

  def receiveData(data: H2Frame.Data): F[Unit] =
    state.get.flatMap { s =>
      val ready =
        if (connectionType == H2Connection.ConnectionType.Client) s.response.tryGet.map(_.isDefined)
        else s.request.tryGet.map(_.isDefined)
      ready.flatMap { initialized =>
        if (!initialized && (s.state == StreamState.Open || s.state == StreamState.HalfClosedLocal))
          rstStream(H2Error.ProtocolError)
        else receiveDataContent(data)
      }
    }

  private def receiveDataContent(data: H2Frame.Data): F[Unit] =
    state
      .modify[DataReceiveResult[F]] { current =>
        current.state match {
          case StreamState.Open | StreamState.HalfClosedLocal =>
            val newSize = current.readWindow - data.flowControlledSize
            val sizeReadOk = current.contentLengthCheck.forall { case (max, read) =>
              data.data.size <= max - read && (!data.endStream || max - read == data.data.size)
            }
            if (newSize < 0)
              (current, DataReceiveResult.StreamError(H2Error.FlowControlError))
            else if (!sizeReadOk)
              (current, DataReceiveResult.StreamError(H2Error.ProtocolError))
            else {
              val next = (current.state, data.endStream) match {
                case (StreamState.Open, true) => StreamState.HalfClosedRemote
                case (StreamState.HalfClosedLocal, true) => StreamState.Closed
                case (other, _) => other
              }
              val isClosed = next == StreamState.Closed
              (
                current.copy(
                  state = next,
                  readWindow = newSize,
                  contentLengthCheck = current.contentLengthCheck.map { case (max, read) =>
                    (max, read + data.data.size)
                  },
                ),
                DataReceiveResult.Accepted(current, isClosed),
              )
            }
          case StreamState.HalfClosedRemote =>
            (current, DataReceiveResult.StreamError(H2Error.StreamClosed))
          case StreamState.Closed => (current, DataReceiveResult.Discard())
          case _ => (current, DataReceiveResult.ConnectionError())
        }
      }
      .flatMap {
        case DataReceiveResult.Accepted(previous, isClosed) =>
          (if (data.data.isEmpty) true.pure[F]
           else
             previous.readBuffer
               .trySend(Right(data.data))
               .flatMap {
                 case Right(false) =>
                   // Closing a full Channel does not release a pending send without a consumer.
                   Temporal[F]
                     .race(
                       previous.readBuffer.send(Right(data.data)),
                       previous.readBuffer.closed,
                     )
                     .map {
                       case Left(Right(_)) => true
                       case _ => false
                     }
                 case Right(queued) => queued.pure[F]
                 case Left(_) => false.pure[F]
               }).flatMap { queued =>
            releaseReadCredit(data.flowControlledSize - data.data.size.toInt).whenA(queued)
          } >>
            (previous.readBuffer.close *> previous.trailWith(List.empty).void)
              .whenA(data.endStream) >>
            onClosed.whenA(isClosed)
        case DataReceiveResult.StreamError(error) => rstStream(error)
        case DataReceiveResult.ConnectionError() => goAway(H2Error.ProtocolError)
        case DataReceiveResult.Discard() => Applicative[F].unit
      }

  def rstStream(error: H2Error): F[Unit] = {
    val rst = error.toRst(id)
    state
      .modify {
        case s if s.state == StreamState.Closed => (s, None)
        case s => (s.copy(state = StreamState.Closed), Some(s))
      }
      .flatMap {
        case Some(s) =>
          enqueue.offer(Chunk.singleton(rst)) >>
            s.cancelWith(s"Sending RstStream, cancelling: $rst") >>
            onClosed
        case None => Applicative[F].unit
      }
  }

  // Broadcast Frame
  // Will eventually allow us to know we can retry if we are above the processed window declared
  def receiveGoAway(goAway: H2Frame.GoAway): F[Unit] =
    state.modify(s => (s.copy(state = StreamState.Closed), s)).flatMap { s =>
      (s.cancelWith(s"Received GoAway, cancelling: $goAway") >> onClosed)
        .whenA(s.state != StreamState.Closed)
    }

  def receiveRstStream(rst: H2Frame.RstStream): F[Unit] =
    state
      .modify {
        case s if s.state == StreamState.Closed => (s, Right(None))
        case s if s.state == StreamState.Idle => (s, Left(()))
        case s => (s.copy(state = StreamState.Closed), Right(Some(s)))
      }
      .flatMap {
        case Left(_) => goAway(H2Error.ProtocolError)
        case Right(Some(s)) =>
          s.cancelWith(s"Received RstStream, cancelling: $rst") >> onClosed
        case Right(None) => Applicative[F].unit
      }

  // Important for telling folks we can send more data
  def receiveWindowUpdate(window: H2Frame.WindowUpdate): F[Unit] =
    Deferred[F, Either[Throwable, Unit]].flatMap { newWriteBlock =>
      state
        .modify[WindowUpdateResult[F]] { s =>
          s.state match {
            case StreamState.HalfClosedLocal | StreamState.Closed =>
              (s, WindowUpdateResult.Ignore())
            case StreamState.Idle | StreamState.ReservedRemote =>
              (s, WindowUpdateResult.ConnectionError())
            case _ if window.windowSizeIncrement == 0 =>
              (s, WindowUpdateResult.StreamError(H2Error.ProtocolError))
            case _ =>
              val newSize = s.writeWindow.toLong + window.windowSizeIncrement.toLong
              if (newSize > Int.MaxValue)
                (s, WindowUpdateResult.StreamError(H2Error.FlowControlError))
              else
                (
                  s.copy(writeBlock = newWriteBlock, writeWindow = newSize.toInt),
                  WindowUpdateResult.Updated(s.writeBlock),
                )
          }
        }
        .flatMap {
          case WindowUpdateResult.Ignore() => Applicative[F].unit
          case WindowUpdateResult.ConnectionError() => goAway(H2Error.ProtocolError)
          case WindowUpdateResult.StreamError(error) => rstStream(error)
          case WindowUpdateResult.Updated(oldWriteBlock) =>
            oldWriteBlock.complete(Either.unit).void
        }
    }

  def modifyWriteWindow(amount: Int): F[Unit] =
    if (amount == 0) Applicative[F].unit
    else
      for {
        newWriteBlock <- Deferred[F, Either[Throwable, Unit]]
        result <- state.modify { s =>
          s.state match {
            case StreamState.HalfClosedLocal | StreamState.ReservedRemote | StreamState.Closed =>
              (s, Right(None))
            case _ =>
              val newSize = s.writeWindow.toLong + amount.toLong
              if (newSize > Int.MaxValue || newSize < Int.MinValue)
                (s, Left(H2Error.FlowControlError))
              else
                (
                  s.copy(writeBlock = newWriteBlock, writeWindow = newSize.toInt),
                  Right(Some(s.writeBlock)),
                )
          }
        }
        _ <- result.fold(goAway, _.traverse_(_.complete(Either.unit).void))
      } yield ()

  def getRequest: F[org.http4s.Request[fs2.Pure]] = state.get.flatMap(_.request.get.rethrow)
  def getResponse: F[org.http4s.Response[fs2.Pure]] = state.get.flatMap(_.response.get.rethrow)

  // Only consumed bytes replenish stream credit. The connection reader never waits on an
  // application body: the advertised window bounds the unbounded channel's retained bytes.
  private def releaseReadCredit(bytes: Int): F[Unit] =
    state
      .modify { s =>
        val consumed = s.consumedSinceUpdate + bytes
        if (
          !s.isClosed && consumed > 0 && consumed >= localSettings.initialWindowSize.windowSize / 2
        ) {
          (s.copy(readWindow = s.readWindow + consumed, consumedSinceUpdate = 0), Some(consumed))
        } else (s.copy(consumedSinceUpdate = consumed), None)
      }
      .flatMap(
        _.traverse_(increment =>
          control.getOrElse(enqueue).offer(Chunk.singleton(H2Frame.WindowUpdate(id, increment)))
        )
      )

  def readBody: Stream[F, Byte] = Stream.force(state.get.map(_.readBuffer.stream)).flatMap {
    case Right(bv) =>
      Stream.chunk(Chunk.byteVector(bv)) ++ Stream.exec(releaseReadCredit(bv.size.toInt))
    case Left(ex) => Stream.raiseError(ex)
  }

}

private[h2] object H2Stream {
  private sealed trait DataReceiveResult[F[_]]
  private object DataReceiveResult {
    final case class Accepted[F[_]](
        previous: State[F],
        closed: Boolean,
    ) extends DataReceiveResult[F]
    final case class StreamError[F[_]](error: H2Error) extends DataReceiveResult[F]
    final case class ConnectionError[F[_]]() extends DataReceiveResult[F]
    final case class Discard[F[_]]() extends DataReceiveResult[F]
  }

  private sealed trait WindowUpdateResult[F[_]]
  private object WindowUpdateResult {
    final case class Ignore[F[_]]() extends WindowUpdateResult[F]
    final case class ConnectionError[F[_]]() extends WindowUpdateResult[F]
    final case class StreamError[F[_]](error: H2Error) extends WindowUpdateResult[F]
    final case class Updated[F[_]](writeBlock: Deferred[F, Either[Throwable, Unit]])
        extends WindowUpdateResult[F]
  }

  final case class State[F[_]](
      state: StreamState,
      writeWindow: Int,
      writeBlock: Deferred[F, Either[Throwable, Unit]],
      readWindow: Int,
      request: Deferred[F, Either[Throwable, org.http4s.Request[fs2.Pure]]],
      response: Deferred[F, Either[Throwable, org.http4s.Response[fs2.Pure]]],
      trailers: Deferred[F, Either[Throwable, org.http4s.Headers]],
      readBuffer: Channel[F, Either[Throwable, ByteVector]],
      contentLengthCheck: Option[(Long, Long)],
      stallStart: Option[FiniteDuration],
      consumedSinceUpdate: Int = 0,
  ) {
    override def toString: String =
      s"H2Stream.State(state=$state, writeWindow=$writeWindow, readWindow=$readWindow, contentLengthCheck=$contentLengthCheck)"

    private[h2] def trailWith(rawHs: List[(String, String)]): F[Boolean] = {
      val hs = Headers(rawHs.map(Header.ToRaw.keyValuesToRaw): _*)
      trailers.complete(Either.right(hs))
    }

    private[H2Stream] def cancelWith(msg: String)(implicit F: Monad[F]): F[Unit] = {
      // Unsure of this, but also unsure about exposing custom throwable
      val ex: Either[Throwable, Nothing] = Left(new CancellationException(msg))
      writeBlock.complete(ex) *>
        request.complete(ex) *>
        response.complete(ex) *>
        readBuffer.closeWithElement(ex) *>
        trailers.complete(ex).void
    }

    def isClosed: Boolean = state == StreamState.HalfClosedRemote || state == StreamState.Closed

  }

  sealed trait StreamState
  object StreamState {
    /*
                                  +--------+
                          send PP |        | recv PP
                        ,--------|  idle  |--------.
                        /         |        |         \
                      v          +--------+          v
                +----------+          |           +----------+
                |          |          | send H /  |          |
        ,------| reserved |          | recv H    | reserved |------.
        |      | (local)  |          |           | (remote) |      |
        |      +----------+          v           +----------+      |
        |          |             +--------+             |          |
        |          |     recv ES |        | send ES     |          |
        |   send H |     ,-------|  open  |-------.     | recv H   |
        |          |    /        |        |        \    |          |
        |          v   v         +--------+         v   v          |
        |      +----------+          |           +----------+      |
        |      |   half   |          |           |   half   |      |
        |      |  closed  |          | send R /  |  closed  |      |
        |      | (remote) |          | recv R    | (local)  |      |
        |      +----------+          |           +----------+      |
        |           |                |                 |           |
        |           | send ES /      |       recv ES / |           |
        |           | send R /       v        send R / |           |
        |           | recv R     +--------+   recv R   |           |
        | send R /  `----------->|        |<-----------'  send R / |
        | recv R                 | closed |               recv R   |
        `----------------------->|        |<----------------------'
                                  +--------+

            send:   endpoint sends this frame
            recv:   endpoint receives this frame

            H:  HEADERS frame (with implied CONTINUATIONs)
            PP: PUSH_PROMISE frame (with implied CONTINUATIONs)
            ES: END_STREAM flag
            R:  RST_STREAM frame
     */

    case object Idle extends StreamState // Transition to ReservedLocal/ReservedRemote/Open
    case object ReservedLocal extends StreamState // Transition to HalfClosedRemote/Closed
    case object ReservedRemote extends StreamState // Transition to HalfClosedLocal/Closed
    case object Open extends StreamState // Transition to HalfClosedRemote/HalfClosedLocal/Closed
    case object HalfClosedRemote extends StreamState // Transition to Closed
    case object HalfClosedLocal extends StreamState // Transition to Closed
    case object Closed extends StreamState // Terminal

  }
}
