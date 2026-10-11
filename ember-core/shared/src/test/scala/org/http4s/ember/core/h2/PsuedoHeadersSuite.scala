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
import org.http4s.headers._
import org.typelevel.ci._

class PsuedoHeadersSuite extends Http4sSuite {

  val connectionHeaders: List[Header.ToRaw] = List(
    Connection.close,
    `Transfer-Encoding`(NonEmptyList.of(TransferCoding.chunked)),
    Upgrade(NonEmptyList.of(Protocol(ci"name", None))),
    `Keep-Alive`.unsafeApply(Some(100L), None, List.empty),
    "Proxy-Connection" -> "keep-alive",
  )

  test("requestToHeaders should remove Connection Headers") {
    val request = Request[fs2.Pure]()
      .putHeaders(connectionHeaders: _*)

    val test = PseudoHeaders.requestToHeaders(request)
    val expected = NonEmptyList.of(
      (":method", "GET", false),
      (":scheme", "https", false),
      (":path", "/", false),
    )

    assertEquals(test, expected)

  }
  test("responseToHeaders should remove Connection Headers") {
    val response = Response[fs2.Pure](Status.Ok)
      .putHeaders(connectionHeaders: _*)

    val test = PseudoHeaders.responseToHeaders(response)
    val expected = NonEmptyList.of(
      (":status", "200", false)
    )
    assertEquals(test, expected)
  }

  test("requestToHeaders should include query in :path") {
    val request = Request[fs2.Pure](uri = Uri().withQueryParam("q", "v") / "foo")

    val test = PseudoHeaders.requestToHeaders(request)
    val expected = NonEmptyList.of(
      (":method", "GET", false),
      (":scheme", "https", false),
      (":path", "/foo?q=v", false),
    )

    assertEquals(test, expected)

  }

  test("requestToHeaders should include query in :path and treat missing path as /") {
    val request = Request[fs2.Pure](uri = Uri().withQueryParam("q", "v"))

    val test = PseudoHeaders.requestToHeaders(request)
    val expected = NonEmptyList.of(
      (":method", "GET", false),
      (":scheme", "https", false),
      (":path", "/?q=v", false),
    )

    assertEquals(test, expected)

  }

  test("requestToHeaders should map OPTIONS requests without path to *") {
    val request = Request[fs2.Pure](method = Method.OPTIONS, uri = Uri())

    val test = PseudoHeaders.requestToHeaders(request)
    val expected = NonEmptyList.of(
      (":method", "OPTIONS", false),
      (":scheme", "https", false),
      (":path", "*", false),
    )

    assertEquals(test, expected)

  }

  private val requestFields =
    List(":method" -> "GET", ":scheme" -> "https", ":path" -> "/", ":authority" -> "example.com")
  private val responseFields = List(":status" -> "200")

  test("request authority is parsed and cookies are reassembled") {
    val result = PseudoHeaders.headersToRequestNoBody(
      requestFields ++ List("cookie" -> "a=b", "cookie" -> "c=d")
    )
    assertEquals(result.flatMap(_.uri.authority).map(_.host.value), Some("example.com"))
    assertEquals(
      result.toList.flatMap(_.headers.headers.filter(_.name == ci"cookie").map(_.value)),
      List("a=b; c=d"),
    )
  }

  test("invalid fields are rejected for both requests and responses") {
    List(
      "" -> "x",
      "Upper" -> "x",
      "x:y" -> "x",
      "x y" -> "x",
      "é" -> "x",
      "x" -> "a\r\nb",
      "x" -> "a\u0000b",
      "x" -> " x",
      "x" -> "x\t",
      "transfer-encoding" -> "chunked",
      "connection" -> "close",
      "upgrade" -> "h2c",
      "keep-alive" -> "1",
      "proxy-connection" -> "close",
      "te" -> "gzip",
      "content-length" -> "-1",
      "content-length" -> "9223372036854775808",
      "content-length" -> "1, 2",
    ).foreach { field =>
      assertEquals(PseudoHeaders.headersToRequestNoBody(requestFields :+ field), None, clue(field))
      assertEquals(
        PseudoHeaders.headersToResponseNoBody(responseFields :+ field),
        None,
        clue(field),
      )
    }
  }

  test("all pseudo headers must be unique, ordered and valid for their message") {
    requestFields.foreach(f =>
      assertEquals(PseudoHeaders.headersToRequestNoBody(f :: requestFields), None)
    )
    assertEquals(PseudoHeaders.headersToRequestNoBody(("x" -> "y") :: requestFields), None)
    assertEquals(PseudoHeaders.headersToResponseNoBody(("x" -> "y") :: responseFields), None)
    assertEquals(PseudoHeaders.headersToResponseNoBody(responseFields :+ (":path" -> "/")), None)
    assertEquals(PseudoHeaders.headersToResponseNoBody(responseFields ++ responseFields), None)
  }

  test("invalid status codes and schemes do not throw") {
    List("", "abc", "+200", "0200", "101", "099", "600", "99999999999").foreach { value =>
      assertEquals(PseudoHeaders.headersToResponseNoBody(List(":status" -> value)), None)
    }
    assertEquals(
      PseudoHeaders.headersToRequestNoBody(requestFields.map {
        case (":scheme", _) => ":scheme" -> "1invalid"
        case other => other
      }),
      None,
    )
  }

  test("authority and path cannot smuggle alternative request targets") {
    List("user@example.com", "example.com/path", "example.com?x", "example.com#x").foreach { value =>
      assertEquals(
        PseudoHeaders.headersToRequestNoBody(
          requestFields.filterNot(_._1 == ":authority") :+
            (":authority" -> value)
        ),
        None,
      )
    }
    List("https://evil.test/", "/#fragment", "relative", "*").foreach { value =>
      assertEquals(
        PseudoHeaders.headersToRequestNoBody(requestFields.map {
          case (":path", _) => ":path" -> value
          case other => other
        }),
        None,
      )
    }
    assertEquals(
      PseudoHeaders.headersToRequestNoBody(requestFields :+ ("host" -> "evil.test")),
      None,
    )
    assert(
      PseudoHeaders.headersToRequestNoBody(requestFields :+ ("host" -> "EXAMPLE.COM:443")).isDefined
    )
  }

  test("CONNECT uses authority and omits scheme and path") {
    val fields = List(":method" -> "CONNECT", ":authority" -> "example.com:443")
    val req = PseudoHeaders.headersToRequestNoBody(fields).get
    assertEquals(req.method, Method.CONNECT)
    assertEquals(PseudoHeaders.requestToHeaders(req).toList.map(h => (h._1, h._2)), fields)
    assertEquals(PseudoHeaders.headersToRequestNoBody(fields :+ (":path" -> "/")), None)
  }

  test("every TE and content-length occurrence is checked") {
    assertEquals(
      PseudoHeaders.headersToRequestNoBody(
        requestFields ++ List("te" -> "trailers", "te" -> "gzip")
      ),
      None,
    )
    assertEquals(
      PseudoHeaders.headersToRequestNoBody(
        requestFields ++ List("content-length" -> "1", "content-length" -> "2")
      ),
      None,
    )
    assertEquals(
      PseudoHeaders.headersToResponseNoBody(responseFields :+ ("te" -> "trailers")),
      None,
    )
  }

  test("outgoing fields remove Connection-nominated headers") {
    val req = Request[fs2.Pure]().putHeaders("Connection" -> "X-Hop", "X-Hop" -> "secret")
    assert(!PseudoHeaders.requestToHeaders(req).exists(_._1 == "x-hop"))
  }

}
