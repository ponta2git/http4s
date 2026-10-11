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
import cats.effect.std.Semaphore
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2._
import fs2.io.IOException
import fs2.io.net._
import org.http4s._
import org.http4s.ember.core.EmberException
import org.http4s.ember.core.Util
import org.typelevel.log4cats.Logger
import scodec.bits._

import java.util.concurrent.CancellationException
import scala.concurrent.TimeoutException
import scala.concurrent.duration._

import H2Frame.Settings.ConnectionSettings.{default => defaultSettings}

private[ember] object H2Server {

  private[ember] def discardResponseBody[F[_]: Concurrent](body: EntityBody[F]): F[Unit] = {
    val F = Concurrent[F]
    // Keep cancellation masked while fs2 registers the body's resource scopes. Polling
    // ordinary effects cancels before they run, while nested acquisition and release
    // masks retain their usual semantics. A chunk checkpoint also stops pure bodies.
    val discard = F.uncancelable { poll =>
      body.chunks
        .evalMap(_ => F.unit)
        .translate(new (F ~> F) {
          def apply[A](fa: F[A]): F[A] = F.canceled >> poll(fa)
        })
        .compile
        .drain
    }
    // A fresh fiber makes the poll cancelable even when called from a Resource finalizer.
    discard.start.flatMap(_.join).flatMap {
      case Outcome.Succeeded(result) => result
      case Outcome.Errored(error) => F.raiseError(error)
      case Outcome.Canceled() => F.unit
    }
  }

  private[h2] def responseResource[F[_]: Concurrent](
      response: F[Response[F]]
  ): Resource[F, (Response[F], F[Unit])] =
    for {
      bodyStarted <- Resource.eval(Ref.of[F, Boolean](false))
      resp <- Resource.makeFull[F, Response[F]](poll => poll(response))(resp =>
        bodyStarted.get.ifM(Applicative[F].unit, discardResponseBody(resp.body))
      )
    } yield (resp, bodyStarted.set(true))

  /*
  2 Mechanism into H2

  TlsContext => Yes => ALPN =>  h2       => HTTP2
                                http/1.1 => HTTP1
                                Nothing  => Http1
                No  =>                      Http1

  This is the tricky one, must read the initial bytes
  if they are the prelude then upgrade, but only on when
  the connection is first established

  Client Prelude Bytes                   => Http2 // Http2-prior-kno
  checkConnectionPreface
  Note: implementations that support HTTP/2 over TLS MUST use protocol
    negotiation in TLS.
    So if TLS is used then this method is not allowed.
   */

  // Call on a new connection for http2-prior-knowledge
  // If left 1.1 if right 2
  def checkConnectionPreface[F[_]: Temporal](
      socket: Socket[F],
      timeout: Duration,
  ): F[Either[ByteVector, Unit]] = {
    def read(acc: ByteVector): F[Either[ByteVector, Unit]] =
      socket.read((Preface.clientBV.size - acc.size).toInt).flatMap {
        case Some(chunk) =>
          val bytes = acc ++ chunk.toByteVector
          if (!Preface.clientBV.startsWith(bytes)) Applicative[F].pure(Left(bytes))
          else if (bytes.size == Preface.clientBV.size) Applicative[F].pure(Right(()))
          else read(bytes)
        case None => new IOException("Input closed during connection preface").raiseError
      }
    Util
      .timeoutMaybe(read(ByteVector.empty), timeout)
      .adaptError { case _: TimeoutException => EmberException.ReadTimeout(timeout) }
  }

  // For Anything that is guaranteed to only be h2 this method will fail
  // unless the connection preface is there. For example after ALPN negotiation
  // on an SSL connection.
  def requireConnectionPreface[F[_]: Temporal](socket: Socket[F], timeout: Duration): F[Unit] =
    checkConnectionPreface(socket, timeout).flatMap {
      case Left(_) => new IllegalArgumentException("Invalid Connection Preface").raiseError
      case Right(unit) => unit.pure[F]
    }

  // This is the full h2 management of a socket
  // AFTER the connection preface.
  // allowing delegation
  def fromSocket[F[_]](
      socket: Socket[F],
      httpApp: HttpApp[F],
      receiveHeadersTimeout: Duration,
      idleTimeout: Duration,
      localSettings: H2Frame.Settings.ConnectionSettings,
      logger: Logger[F],
  )(implicit F: Async[F]): Resource[F, Unit] =
    fromSocket(socket, httpApp, receiveHeadersTimeout, idleTimeout, localSettings, logger, F.never)

  def fromSocket[F[_]](
      socket: Socket[F],
      httpApp: HttpApp[F],
      receiveHeadersTimeout: Duration,
      idleTimeout: Duration,
      localSettings: H2Frame.Settings.ConnectionSettings,
      logger: Logger[F],
      shutdown: F[Unit],
  )(implicit F: Async[F]): Resource[F, Unit] = {

    val initialRemoteSettings =
      defaultSettings.copy(maxConcurrentStreams = H2Frame.Settings.SettingsMaxConcurrentStreams(-1))

    def holdWhileOpen(stateRef: Ref[F, H2Connection.State[F]]): F[Unit] =
      F.sleep(1.second) >> stateRef.get.map(_.closed).ifM(F.unit, holdWhileOpen(stateRef))

    def awaitDrained(h2: H2Connection[F], activeHandlers: Ref[F, Int]): F[Unit] =
      h2.mapRef.get
        .flatMap(_.values.toList.traverse(_.state.get.map(_.state)))
        .flatMap(states => activeHandlers.get.tupleLeft(states))
        .flatMap { case (states, active) =>
          if (active == 0 && states.forall(_ == H2Stream.StreamState.Closed))
            // END_STREAM changes stream state when queued. Wait for the socket write
            // before ending the connection's Resource and canceling its writer.
            h2.flushWrites >> h2.state.update(_.copy(closed = true))
          else F.sleep(10.millis) >> awaitDrained(h2, activeHandlers)
        }

    def initH2Connection: F[H2Connection[F]] = for {
      ref <- Concurrent[F].ref(Map[Int, H2Stream[F]]())
      stateRef <- H2Connection.initState[F](
        initialRemoteSettings,
        defaultSettings.initialWindowSize,
        defaultSettings.initialWindowSize,
      )
      queue <- cats.effect.std.Queue.bounded[F, H2Write[F]](128)
      control <- cats.effect.std.Queue.bounded[F, H2Write[F]](128)
      hpack <- Hpack.create[F](
        localSettings.maxHeaderListSize.fold(65536)(_.listSize),
        H2Connection.abort(stateRef, ref),
      )
      settingsAck <- Deferred[F, Either[Throwable, H2Frame.Settings.ConnectionSettings]]
      streamCreationLock <- Semaphore[F](1)
      // data <- Resource.eval(cats.effect.std.Queue.unbounded[F, Frame.Data])
      created <- cats.effect.std.Queue.unbounded[F, Int]
      closed <- cats.effect.std.Queue.unbounded[F, Int]
    } yield new H2Connection(
      socket.peerAddress,
      H2Connection.ConnectionType.Server,
      receiveHeadersTimeout,
      idleTimeout,
      localSettings,
      ref,
      stateRef,
      queue,
      created,
      closed,
      hpack,
      streamCreationLock.permit,
      settingsAck,
      ByteVector.empty,
      socket,
      logger,
      Some(control),
    )

    def clearClosedStreams(h2: H2Connection[F]): F[Unit] =
      Stream
        .fromQueueUnterminated(h2.closedStreams)
        .map(i =>
          Stream.eval(
            // Max Time After Close We Will Still Accept Messages
            (Temporal[F].sleep(1.seconds) >>
              h2.mapRef.update(m => m - i)).timeout(15.seconds).attempt.start
          )
        )
        .parJoin(localSettings.maxConcurrentStreams.maxConcurrency)
        .compile
        .drain

    def processCreatedStream(
        h2: H2Connection[F],
        streamIx: Int,
        maxStreams: Semaphore[F],
    ): F[Unit] = {
      def fulfillPushPromises(resp: Response[F]): F[Unit] = {
        def sender(req: Request[Pure]): Resource[F, H2Stream[F]] =
          Resource.makeFull[F, H2Stream[F]] { poll =>
            poll(h2.streamCreateAndHeaders.use { _ =>
              F.uncancelable { poll =>
                poll(h2.initiateLocalStream).flatMap { stream =>
                  poll(stream.sendPushPromise(streamIx, PseudoHeaders.requestToHeaders(req)))
                    .as(stream)
                    .onCancel(stream.rstStream(H2Error.Cancel))
                }
              }
            })
          }(_.rstStream(H2Error.Cancel))

        def respond(req: Request[Pure], stream: H2Stream[F]): F[Unit] =
          F.race(
            stream.awaitCancellation,
            responseResource(httpApp(req.covary[F])).use { case (pushedResponse, beforeBody) =>
              val noContent = req.method == Method.HEAD || !pushedResponse.status.isEntityAllowed
              if (pushedResponse.status.code < 200) stream.rstStream(H2Error.InternalError)
              else
                stream.sendHeaders(
                  PseudoHeaders.responseToHeaders(pushedResponse),
                  endStream = noContent,
                ) >>
                  (stream.sendMessageBody(pushedResponse, beforeBody) >> stream.sendTrailerHeaders(
                    pushedResponse
                  )).unlessA(noContent)
            },
          ).void

        resp.attributes
          .lookup(H2Keys.PushPromises)
          .traverse_ { (l: List[Request[Pure]]) =>
            h2.state.get.flatMap {
              case s
                  if s.draining.isEmpty && s.remoteSettings.enablePush.isEnabled && s.remoteSettings.maxConcurrentStreams.maxConcurrency != 0 =>
                l.filter(req =>
                  (req.method == Method.GET || req.method == Method.HEAD) && req.contentLength
                    .forall(_ == 0)
                ).traverse_(req => sender(req).use(stream => respond(req, stream)))
              case _ => Applicative[F].unit
            }
          }
          .recoverWith { case error: H2Connection.KillWithoutMessage =>
            // Shutdown may start after the check above while waiting for push capacity.
            // Skip the new push, while still completing the accepted parent response.
            h2.state.get.flatMap(s => if (s.draining.isDefined) F.unit else F.raiseError(error))
          }
      }

      h2.mapRef.get.flatMap(
        _.get(streamIx).traverse_ { stream =>
          val permit =
            if (streamIx % 2 != 0) maxStreams.permit else Resource.unit[F]

          F.race(
            stream.awaitCancellation,
            permit.use { _ =>
              for {
                req <- stream.getRequest.map(_.covary[F].withBodyStream(stream.readBody))
                _ <- responseResource(httpApp(req)).use { case (resp, beforeBody) =>
                  val noContent = req.method == Method.HEAD || !resp.status.isEntityAllowed
                  if (resp.status.code < 200) stream.rstStream(H2Error.InternalError)
                  else
                    fulfillPushPromises(resp).whenA(noContent) >> stream.sendHeaders(
                      PseudoHeaders.responseToHeaders(resp),
                      endStream = noContent,
                    ) >>
                      fulfillPushPromises(resp).unlessA(noContent) >>
                      (stream.sendMessageBody(resp, beforeBody) >> stream.sendTrailerHeaders(
                        resp
                      ))
                        .unlessA(noContent)
                }
                _ <- stream.state.get.flatMap { streamState =>
                  // The response no longer owns a request body consumer. RFC 9113, section 8.1
                  // permits NO_ERROR after the complete response when the request is unfinished.
                  stream
                    .rstStream(H2Error.NoError)
                    .whenA(streamState.state == H2Stream.StreamState.HalfClosedLocal) >>
                    // Final DATA may already have closed the stream while blocked on this buffer.
                    streamState.readBuffer.close.void
                }
              } yield ()

            },
          ).void
            .onError {
              case _: CancellationException => F.unit
              case _ => stream.rstStream(H2Error.InternalError)
            }
            .guarantee(h2.mapRef.update(_ - streamIx))
        }
      )
    }

    def processCreatedStreams(
        h2: H2Connection[F],
        maxStreams: Semaphore[F],
        activeHandlers: Ref[F, Int],
    ): F[Unit] =
      Stream
        .fromQueueUnterminated(h2.createdStreams)
        .parEvalMapUnbounded(i =>
          Resource
            .make(activeHandlers.update(_ + 1))(_ => activeHandlers.update(_ - 1))
            .use(_ => processCreatedStream(h2, i, maxStreams))
            .handleErrorWith {
              case e: CancellationException =>
                logger.debug(e)("Stream canceled before processing")
              case e: TimeoutException =>
                logger.debug(e)("Connection timed out")
              case e =>
                logger.error(e)(s"Error while processing stream")
            }
        )
        .compile
        .drain
        .onError { case e => logger.error(e)(s"Server Connection Processing Halted") }

    val settingsFrame = H2Frame.Settings.ConnectionSettings.toSettings(localSettings)

    for {
      h2 <- Resource.eval(initH2Connection)
      _ <- h2.writeLoop.compile.drain.background
      _ <- Resource.eval(socket.write(Chunk.byteVector(H2Frame.toByteVector(settingsFrame))))
      _ <- h2.readLoop.background
      maxStreams <- Resource.eval(
        Semaphore[F](localSettings.maxConcurrentStreams.maxConcurrency.toLong)
      )
      activeHandlers <- Resource.eval(Ref.of[F, Int](0))
      _ <- clearClosedStreams(h2).background
      _ <- processCreatedStreams(h2, maxStreams, activeHandlers).background
      _ <- Resource.eval(
        F.race(
          holdWhileOpen(h2.state),
          shutdown >> h2.beginGracefulShutdown >> awaitDrained(h2, activeHandlers),
        ).void
      )
    } yield ()
  }
}
