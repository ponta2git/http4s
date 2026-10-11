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
import cats.effect.IO
import cats.effect.Ref
import com.comcast.ip4s._
import fs2.Chunk
import fs2.Stream
import fs2.io.net.Network
import org.http4s._

import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration._

class HeadResponseSuite extends Http4sSuite {
  private val content = "recovered"
  private val unexpectedBody =
    Stream.eval(IO.raiseError[Byte](new AssertionError("HEAD body effect evaluated")))

  List(
    ("empty body without representation length", Response[IO](Status.MethodNotAllowed), None),
    (
      "explicit representation length",
      Response[IO]().withEntity(content),
      Some(content.length.toString),
    ),
    ("unknown representation length", Response[IO]().withBodyStream(unexpectedBody), None),
    (
      "chunked representation with trailers",
      Response[IO]()
        .withBodyStream(unexpectedBody)
        .putHeaders("Transfer-Encoding" -> "chunked", "Trailer" -> "X-Trailer")
        .withTrailerHeaders(IO.raiseError(new AssertionError("HEAD trailers evaluated"))),
      None,
    ),
  ).foreach { case (name, headResponse, expectedLength) =>
    test(s"HEAD preserves metadata and the following response: $name") {
      for {
        finalized <- Ref.of[IO, Boolean](false)
        response =
          if (headResponse.body eq EmptyBody) headResponse
          else headResponse.withBodyStream(headResponse.body.onFinalize(finalized.set(true)))
        app = Kleisli[IO, Request[IO], Response[IO]] { request =>
          IO.pure(
            if (request.method == Method.HEAD) response
            else Response[IO]().withEntity(content)
          )
        }
        _ <- EmberServerBuilder
          .default[IO]
          .withHost(host"127.0.0.1")
          .withPort(port"0")
          .withHttpApp(app)
          .build
          .use { server =>
            Network[IO].connect(server.addressIp4s).use { socket =>
              val requests =
                "HEAD / HTTP/1.1\r\nHost: localhost\r\n\r\n" +
                  "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
              for {
                _ <- socket.write(Chunk.array(requests.getBytes(UTF_8)))
                wire <- socket.reads.through(fs2.text.utf8.decode).compile.string
                wasFinalized <- finalized.get
              } yield {
                val headerEnd = wire.indexOf("\r\n\r\n")
                assert(headerEnd >= 0, wire)
                val headers = wire.take(headerEnd)
                val contentLength = headers
                  .split("\r\n")
                  .iterator
                  .find(_.toLowerCase(java.util.Locale.ROOT).startsWith("content-length:"))
                  .map(_.drop("content-length:".length).trim)
                assertEquals(contentLength, expectedLength)
                assert(wire.drop(headerEnd + 4).startsWith("HTTP/1.1 200 OK\r\n"), wire)
                assert(wire.endsWith("\r\n\r\n" + content), wire)
                assert(
                  (headResponse.body eq EmptyBody) || wasFinalized,
                  "suppressed response body was not finalized",
                )
              }
            }
          }
          .timeout(5.seconds)
      } yield ()
    }
  }
}
