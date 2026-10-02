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

package org.http4s
package ember.core.h2

import cats.data.NonEmptyList
import cats.effect.Deferred
import cats.effect.IO
import cats.effect.Ref
import cats.effect.std.Queue
import cats.effect.testkit.TestControl
import cats.syntax.all._
import org.http4s.ember.core.EmberException
import org.http4s.laws.discipline.arbitrary._
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalacheck.effect.PropF.forAllF
import org.typelevel.ci.CIString
import scodec.bits.ByteVector

import scala.concurrent.duration.DurationInt

class HpackSuite extends Http4sSuite {

  implicit val http4sTestingArbitraryForRawHeader: Arbitrary[Header.Raw] =
    Arbitrary {
      for {
        token <- Gen.frequency(
          8 -> genToken,
          1 -> Gen.nonEmptyBuildableOf[String, Char](Gen.asciiChar),
        )
        value <- genFieldValue
      } yield Header.Raw(CIString(token), value)
    }

  test("hpack round-trip") {
    forAllF { (headers: NonEmptyList[Header.Raw]) =>
      for {
        hpack <- Hpack.create[IO](65635)
        bv <- hpack.encodeHeaders(headers.map { case Header.Raw(n, v) => (n.toString, v, false) })
        decoded <- hpack.decodeHeaders(bv)
      } yield assertEquals(decoded, headers.map { case Header.Raw(n, v) => (n.toString, v) })
    }
  }

  test("oversized headers are rejected") {
    interceptIO[EmberException.MessageTooLong] {
      for {
        hpack <- Hpack.create[IO](4)
        bv <- hpack.encodeHeaders(NonEmptyList.one(("key", "value", false)))
        _ <- hpack.decodeHeaders(bv)
      } yield ()
    }
  }

  test("discarded headers update decoder state without requiring fields") {
    for {
      hpack <- Hpack.create[IO](65635)
      first <- hpack.encodeHeaders(NonEmptyList.one(("x-dynamic", "value", false)))
      second <- hpack.encodeHeaders(NonEmptyList.one(("x-dynamic", "value", false)))
      _ <- hpack.decodeHeadersAndDiscard(ByteVector.empty)
      _ <- hpack.decodeHeadersAndDiscard(first)
      decoded <- hpack.decodeHeaders(second)
    } yield assertEquals(decoded, NonEmptyList.one(("x-dynamic", "value")))
  }

  test("encoding and sending header blocks share one connection order") {
    TestControl.executeEmbed {
      val headers = NonEmptyList.one(("x-dynamic", "value", false))
      for {
        encoder <- Hpack.create[IO](1024)
        decoder <- Hpack.create[IO](1024)
        outgoing <- Queue.unbounded[IO, ByteVector]
        entered <- Deferred[IO, Unit]
        resume <- Deferred[IO, Unit]
        secondEncoded <- Ref[IO].of(false)
        _ <- encoder
          .encodeHeadersWith(headers)(bytes =>
            entered.complete(()).void >> resume.get >> outgoing.offer(bytes)
          )
          .background
          .use { first =>
            entered.get >>
              encoder
                .encodeHeadersWith(headers)(bytes =>
                  secondEncoded.set(true) >> outgoing.offer(bytes)
                )
                .background
                .use { second =>
                  IO.sleep(1.second) >> assertIO(secondEncoded.get, false) >>
                    resume.complete(()) >> first.flatMap(_.embedNever) >>
                    second.flatMap(_.embedNever)
                }
          }
        blocks <- outgoing.take.replicateA(2)
        decoded <- blocks.traverse(decoder.decodeHeaders)
      } yield assertEquals(decoded, List.fill(2)(headers.map(h => (h._1, h._2))))
    }
  }

  test("canceling a blocked header send poisons the encoder without blocking cancellation") {
    TestControl.executeEmbed {
      val headers = NonEmptyList.one(("x-dynamic", "value", false))
      for {
        failures <- Ref[IO].of(0)
        encoder <- Hpack.create[IO](1024, failures.update(_ + 1))
        outgoing <- Queue.bounded[IO, ByteVector](1)
        _ <- outgoing.offer(ByteVector.empty)
        entered <- Deferred[IO, Unit]
        _ <- encoder
          .encodeHeadersWith(headers)(bytes => entered.complete(()).void >> outgoing.offer(bytes))
          .background
          .use(_ => entered.get)
          .timeout(1.second)
        subsequent <- encoder.encodeHeaders(headers).attempt.timeout(1.second)
        count <- failures.get
        queued <- outgoing.take
        extra <- outgoing.tryTake
      } yield {
        assert(subsequent.isLeft, clue(subsequent))
        assertEquals(count, 1)
        assertEquals(queued, ByteVector.empty)
        assertEquals(extra, None)
      }
    }
  }

  test("canceling while waiting for the encoder lock leaves it usable") {
    TestControl.executeEmbed {
      val headers = NonEmptyList.one(("x-dynamic", "value", false))
      for {
        failures <- Ref[IO].of(0)
        encoder <- Hpack.create[IO](1024, failures.update(_ + 1))
        decoder <- Hpack.create[IO](1024)
        outgoing <- Queue.unbounded[IO, ByteVector]
        entered <- Deferred[IO, Unit]
        resume <- Deferred[IO, Unit]
        _ <- encoder
          .encodeHeadersWith(headers)(bytes =>
            entered.complete(()).void >> resume.get >> outgoing.offer(bytes)
          )
          .background
          .use { first =>
            entered.get >>
              encoder
                .encodeHeadersWith(headers)(outgoing.offer)
                .background
                .use(_ => IO.sleep(1.second)) >>
              resume.complete(()) >> first.flatMap(_.embedNever)
          }
        _ <- encoder.encodeHeadersWith(headers)(outgoing.offer)
        blocks <- outgoing.take.replicateA(2)
        decoded <- blocks.traverse(decoder.decodeHeaders)
        count <- failures.get
      } yield {
        assertEquals(decoded, List.fill(2)(headers.map(h => (h._1, h._2))))
        assertEquals(count, 0)
      }
    }
  }

  test("a failed header send poisons the encoder and reports the failure once") {
    val headers = NonEmptyList.one(("x-dynamic", "value", false))
    val failure = new RuntimeException("send failed")
    for {
      failures <- Ref[IO].of(0)
      encoder <- Hpack.create[IO](1024, failures.update(_ + 1))
      result <- encoder.encodeHeadersWith(headers)(_ => IO.raiseError[Unit](failure)).attempt
      subsequent <- encoder.encodeHeaders(headers).attempt
      count <- failures.get
    } yield {
      assertEquals(result, Left(failure))
      assert(subsequent.isLeft, clue(subsequent))
      assertEquals(count, 1)
    }
  }

}
