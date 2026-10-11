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
  def updateEncoderTableSize(size: Int): F[Unit]

  def decodeHeaders(bv: ByteVector): F[List[(String, String)]]

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
    pending <- Ref.of[F, List[Int]](Nil)
    e <- Sync[F].delay(new Encoder(4096))
    d <- Sync[F].delay(new Decoder(maxHeaderListSize, 4096))
  } yield new Impl(eLock, eFailed, onEncodeFailure, pending, e, dLock, d, maxHeaderListSize.toLong)

  private class Impl[F[_]: Async](
      encodeLock: Mutex[F],
      encodeFailed: Ref[F, Boolean],
      onEncodeFailure: F[Unit],
      pendingTableSizes: Ref[F, List[Int]],
      tEncoder: Encoder,
      decodeLock: Mutex[F],
      tDecoder: Decoder,
      maxHeaderListSize: Long,
  ) extends Hpack[F] {
    def updateEncoderTableSize(size: Int): F[Unit] =
      encodeLock.lock.surround(pendingTableSizes.update { sizes =>
        // The encoder may choose a smaller table than the peer's unsigned limit.
        val limit = math.min(size.toLong & 0xffffffffL, 4096L).toInt
        List((limit :: sizes).min, limit)
      })

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
              pendingTableSizes
                .getAndSet(Nil)
                .flatMap { sizes =>
                  Sync[F]
                    .delay {
                      val out = new ByteVectorOutputStream(16)
                      sizes.distinct.foreach(tEncoder.setMaxHeaderTableSize(out, _))
                      out.toByteVector()
                    }
                    .flatMap(prefix =>
                      Hpack.encodeHeaders[F](tEncoder, headers.toList).map(prefix ++ _)
                    )
                }
                .flatMap(bytes => poll(send(bytes)))
                .guaranteeCase {
                  case Outcome.Succeeded(_) => Async[F].unit
                  case _ => encodeFailed.set(true) >> onEncodeFailure
                }
          }
        }
      }
    def decodeHeaders(bv: ByteVector): F[List[(String, String)]] =
      decodeLock.lock.surround(Hpack.decodeHeaders[F](tDecoder, bv, maxHeaderListSize))
    def decodeHeadersAndDiscard(bv: ByteVector): F[Unit] =
      decodeLock.lock.surround(Hpack.decodeHeadersAndDiscard[F](tDecoder, bv, maxHeaderListSize))

  }

  def decodeHeaders[F[_]: Sync](
      tDecoder: Decoder,
      bv: ByteVector,
      maxHeaderListSize: Long,
  ): F[List[(String, String)]] = Sync[F].delay {
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
    decoded
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
    validateBlock(bv)
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

  // The underlying decoder accepts incremental input and endHeaderBlock resets its parser,
  // even for incomplete representations. Validate framing before allowing it to mutate state.
  private def validateBlock(bytes: ByteVector): Unit = {
    var offset = 0L
    var fieldsStarted = false
    def invalid(): Nothing = throw new java.io.IOException("Invalid HPACK field block")
    def integer(bits: Int): Long = {
      if (offset >= bytes.size) invalid()
      val mask = (1 << bits) - 1
      var value = (bytes(offset) & mask).toLong
      offset += 1
      if (value == mask) {
        var shift = 0
        var more = true
        while (more) {
          if (offset >= bytes.size || shift > 28) invalid()
          val next = bytes(offset) & 0xff
          offset += 1
          value += (next & 0x7f).toLong << shift
          if (value > Int.MaxValue) invalid()
          more = (next & 0x80) != 0
          shift += 7
        }
      }
      value
    }
    def string(): Unit = {
      val length = integer(7)
      if (length > bytes.size - offset) invalid()
      offset += length
    }
    while (offset < bytes.size) {
      val first = bytes(offset) & 0xff
      if ((first & 0x80) != 0) {
        if (integer(7) == 0) invalid()
        fieldsStarted = true
      } else if ((first & 0xe0) == 0x20) {
        if (fieldsStarted) invalid()
        integer(5)
        ()
      } else {
        val index = integer(if ((first & 0x40) != 0) 6 else 4)
        if (index == 0) string()
        string()
        fieldsStarted = true
      }
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
