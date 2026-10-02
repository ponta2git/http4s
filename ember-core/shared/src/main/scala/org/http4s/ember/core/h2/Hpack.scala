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

import cats.data._
import cats.effect._
import cats.effect.std._
import cats.effect.syntax.all._
import cats.syntax.all._
import org.http4s.ember.core.EmberException
import scodec.bits._

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

private[h2] trait Hpack[F[_]] {
  def encodeHeaders(headers: NonEmptyList[(String, String, Boolean)]): F[ByteVector]

  /** Keeps encoder updates in the same order as the corresponding outgoing header blocks. */
  def encodeHeadersWith[A](headers: NonEmptyList[(String, String, Boolean)])(
      send: ByteVector => F[A]
  ): F[A]
  def decodeHeaders(bv: ByteVector): F[NonEmptyList[(String, String)]]

  /** Advances HPACK decoder state without materializing a field list. */
  def decodeHeadersAndDiscard(bv: ByteVector): F[Unit]
}

private[h2] object Hpack extends HpackPlatform {
  def create[F[_]: Async](maxHeaderListSize: Int): F[Hpack[F]] =
    create(maxHeaderListSize, Async[F].unit)

  def create[F[_]: Async](maxHeaderListSize: Int, onEncodeFailure: F[Unit]): F[Hpack[F]] = for {
    eLock <- Mutex[F]
    eFailed <- Ref.of[F, Boolean](false)
    dLock <- Mutex[F]
    e <- Sync[F].delay(new Encoder(4096))
    d <- Sync[F].delay(new Decoder(maxHeaderListSize, 4096))
  } yield new Impl(eLock, eFailed, onEncodeFailure, e, dLock, d, maxHeaderListSize.toLong)

  private class Impl[F[_]: Async](
      encodeLock: Mutex[F],
      encodeFailed: Ref[F, Boolean],
      onEncodeFailure: F[Unit],
      tEncoder: Encoder,
      decodeLock: Mutex[F],
      tDecoder: Decoder,
      maxHeaderListSize: Long,
  ) extends Hpack[F] {
    def encodeHeaders(headers: NonEmptyList[(String, String, Boolean)]): F[ByteVector] =
      encodeHeadersWith(headers)(Async[F].pure(_))

    def encodeHeadersWith[A](headers: NonEmptyList[(String, String, Boolean)])(
        send: ByteVector => F[A]
    ): F[A] =
      encodeLock.lock.surround {
        Async[F].uncancelable { poll =>
          encodeFailed.get.flatMap {
            case true =>
              new IllegalStateException("HPACK encoder is no longer usable").raiseError[F, A]
            case false =>
              // Queue offers must remain cancelable when a stalled writer has filled the queue.
              // Once encoding has advanced the dynamic table, losing its block invalidates the
              // connection. Poison the encoder before releasing the lock to any waiting sender.
              Hpack
                .encodeHeaders[F](tEncoder, headers.toList)
                .flatMap(bytes => poll(send(bytes)))
                .guaranteeCase {
                  case Outcome.Succeeded(_) => Async[F].unit
                  case _ => encodeFailed.set(true) >> onEncodeFailure
                }
          }
        }
      }
    def decodeHeaders(bv: ByteVector): F[NonEmptyList[(String, String)]] =
      decodeLock.lock.surround(Hpack.decodeHeaders[F](tDecoder, bv, maxHeaderListSize))
    def decodeHeadersAndDiscard(bv: ByteVector): F[Unit] =
      decodeLock.lock.surround(Hpack.decodeHeadersAndDiscard[F](tDecoder, bv, maxHeaderListSize))

  }

  def decodeHeaders[F[_]: Sync](
      tDecoder: Decoder,
      bv: ByteVector,
      maxHeaderListSize: Long,
  ): F[NonEmptyList[(String, String)]] = Sync[F].delay {
    val buffer = List.newBuilder[(String, String)]
    decodeHeaderBlock(tDecoder, bv, maxHeaderListSize) { (name, value) =>
      buffer.+=(
        new String(name, StandardCharsets.ISO_8859_1) -> new String(
          value,
          StandardCharsets.ISO_8859_1,
        )
      )
    }
    val decoded = buffer.result()
    NonEmptyList.fromListUnsafe(decoded)
  }

  def decodeHeadersAndDiscard[F[_]: Sync](
      tDecoder: Decoder,
      bv: ByteVector,
      maxHeaderListSize: Long,
  ): F[Unit] =
    Sync[F].delay(decodeHeaderBlock(tDecoder, bv, maxHeaderListSize)((_, _) => ()))

  private def decodeHeaderBlock(
      tDecoder: Decoder,
      bv: ByteVector,
      maxHeaderListSize: Long,
  )(onHeader: (Array[Byte], Array[Byte]) => Unit): Unit = {
    var decodedSize = 0L
    val listener = new HeaderListener {
      def addHeader(name: Array[Byte], value: Array[Byte], sensitive: Boolean): Unit = {
        // The HPACK decoder implementation does not track the 32 byte overhead, nor does it count
        // indexed references. So we need to do the accounting ourselves.
        decodedSize += name.length + value.length + 32 // + 32 per overhead in HTTP/2 spec
        if (decodedSize > maxHeaderListSize) {
          throw EmberException.MessageTooLong(maxHeaderListSize.toInt)
        }
        onHeader(name, value)
      }
    }

    tDecoder.decode(bv.toInputStream, listener)
    if (tDecoder.endHeaderBlock()) {
      throw EmberException.MessageTooLong(maxHeaderListSize.toInt)
    }
  }

  def encodeHeaders[F[_]: Sync](
      tEncoder: Encoder,
      headers: List[(String, String, Boolean)],
  ): F[ByteVector] = Sync[F].delay {
    val os = new ByteVectorOutputStream(1024)
    headers.foreach { h =>
      tEncoder.encodeHeader(
        os,
        h._1.getBytes(StandardCharsets.ISO_8859_1),
        h._2.getBytes(StandardCharsets.ISO_8859_1),
        h._3,
      )
    }
    os.toByteVector()
  }

  private final class ByteVectorOutputStream(size: Int) extends ByteArrayOutputStream(size) {
    def toByteVector(): ByteVector = ByteVector.view(buf, 0, count)
  }

}
