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
import cats.syntax.all._
import org.http4s._
import org.typelevel.ci.CIString

/** HTTP/2 field validation (RFC 9113, sections 8.1 through 8.5). */
private[h2] object PseudoHeaders {
  val METHOD = ":method"
  val SCHEME = ":scheme"
  val PATH = ":path"
  val AUTHORITY = ":authority"
  val STATUS = ":status"
  val requestPsedo: Set[String] = Set(METHOD, SCHEME, PATH, AUTHORITY)

  val connectionHeadersFields: Set[CIString] = Set(
    CIString("transfer-encoding"),
    CIString("connection"),
    CIString("upgrade"),
    CIString("keep-alive"),
    CIString("proxy-connection"),
  )

  private def token(c: Char): Boolean =
    c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || "!#$%&'*+-.^_`|~".contains(c)

  private def validValue(value: String): Boolean =
    value.forall(c => c == '\t' || c >= ' ' && c != 0x7f && c <= 0xff) &&
      !value.headOption.exists(c => c == ' ' || c == '\t') &&
      !value.lastOption.exists(c => c == ' ' || c == '\t')

  private def validFields(fields: List[(String, String)], request: Boolean): Boolean =
    fields.forall { case (name, value) =>
      name.nonEmpty && name.forall(token) && validValue(value) &&
      !connectionHeadersFields(CIString(name)) &&
      (name != "te" || request && value.equalsIgnoreCase("trailers"))
    } && validContentLength(fields)

  private def validContentLength(fields: List[(String, String)]): Boolean = {
    val lengths = fields.collect { case ("content-length", value) => value }
    // Reject ambiguous framing, including duplicate or comma-separated lengths.
    lengths.size <= 1 && lengths.forall(v =>
      v.nonEmpty && v.forall(c => c >= '0' && c <= '9') &&
        scala.util.Try(v.toLong).toOption.exists(_ >= 0)
    )
  }

  private def splitFields(
      fields: List[(String, String)],
      allowed: Set[String],
      request: Boolean,
  ): Option[(Map[String, String], List[(String, String)])] = {
    val (pseudo, regular) = fields.span(_._1.startsWith(":"))
    cats
      .Alternative[Option]
      .guard(
        pseudo.forall { case (name, value) => allowed(name) && validValue(value) } &&
          pseudo.map(_._1).distinct.size == pseudo.size && validFields(regular, request)
      )
      .as(pseudo.toMap -> regular)
  }

  private def toHeaders(fields: List[(String, String)]): Headers = {
    val cookies = fields.collect { case ("cookie", value) => value }
    val combined =
      if (cookies.size <= 1) fields
      else fields.filterNot(_._1 == "cookie") :+ ("cookie" -> cookies.mkString("; "))
    Headers(combined.map { case (name, value) => Header.Raw(CIString(name), value) })
  }

  private def parseAuthority(value: String): Option[Uri.Authority] =
    Uri
      .fromString("//" + value)
      .toOption
      .filter(u => u.path.isEmpty && u.query.isEmpty && u.fragment.isEmpty)
      .flatMap(_.authority)
      .filter(a => a.userInfo.isEmpty && a.host.value.nonEmpty)

  def extractAuthority(headers: List[(String, String)]): Option[Uri.Authority] =
    findWithNoDuplicates(headers)(_._1 == AUTHORITY).flatMap(h => parseAuthority(h._2))

  def headersToRequestNoBody(fields: NonEmptyList[(String, String)]): Option[Request[fs2.Pure]] =
    headersToRequestNoBody(fields.toList)

  def headersToRequestNoBody(fields: List[(String, String)]): Option[Request[fs2.Pure]] =
    for {
      split <- splitFields(fields, requestPsedo, request = true)
      (pseudo, regular) = split
      method <- pseudo.get(METHOD).flatMap(Method.fromString(_).toOption)
      hosts = regular.collect { case ("host", value) => value }
      _ <- cats.Alternative[Option].guard(hosts.size <= 1).as(())
      authority <- pseudo.get(AUTHORITY).orElse(hosts.headOption).traverse(parseAuthority)
      _ <- cats
        .Alternative[Option]
        .guard(
          hosts.forall(h =>
            parseAuthority(h).exists(a =>
              authority.exists(b =>
                a.host.value.equalsIgnoreCase(b.host.value) &&
                  a.port.orElse(defaultPort(pseudo.get(SCHEME))) == b.port.orElse(
                    defaultPort(pseudo.get(SCHEME))
                  )
              )
            )
          )
        )
        .as(())
      uri <-
        if (method == Method.CONNECT) {
          cats
            .Alternative[Option]
            .guard(
              !pseudo.contains(SCHEME) && !pseudo.contains(PATH) &&
                pseudo.contains(AUTHORITY) && authority.exists(_.port.isDefined)
            )
            .as(
              Uri(authority = authority)
            )
        } else
          for {
            scheme <- pseudo.get(SCHEME).flatMap(Uri.Scheme.fromString(_).toOption)
            path <- pseudo.get(PATH)
            _ <- cats
              .Alternative[Option]
              .guard(path.startsWith("/") || path == "*" && method == Method.OPTIONS)
              .as(())
            // Parse as a path, so a leading // cannot be interpreted as an authority.
            target <- Uri
              .fromString("http://placeholder" + (if (path == "*") "/" else path))
              .toOption
            _ <- cats.Alternative[Option].guard(target.fragment.isEmpty).as(())
          } yield target.copy(
            scheme = Some(scheme),
            authority = authority,
            path = if (path == "*") Uri.Path.unsafeFromString("*") else target.path,
          )
    } yield Request(method, uri, HttpVersion.`HTTP/2`, toHeaders(regular))

  private def defaultPort(scheme: Option[String]): Option[Int] =
    scheme.map(_.toLowerCase(java.util.Locale.ROOT)) match {
      case Some("https") => Some(443)
      case Some("http") => Some(80)
      case _ => None
    }

  def headersToResponseNoBody(fields: NonEmptyList[(String, String)]): Option[Response[fs2.Pure]] =
    headersToResponseNoBody(fields.toList)

  def headersToResponseNoBody(fields: List[(String, String)]): Option[Response[fs2.Pure]] =
    for {
      split <- splitFields(fields, Set(STATUS), request = false)
      (pseudo, regular) = split
      value <- pseudo.get(STATUS)
      _ <- cats
        .Alternative[Option]
        .guard(value.length == 3 && value.forall(c => c >= '0' && c <= '9'))
        .as(())
      status <- Status
        .fromInt(value.toInt)
        .toOption
        .filter(s => s.code >= 100 && s.code < 600 && s.code != 101)
    } yield Response(status, HttpVersion.`HTTP/2`, toHeaders(regular))

  def validTrailers(fields: List[(String, String)]): Boolean =
    validFields(fields, request = false) && !fields.exists { case (name, _) =>
      name == "content-length" || name == "host"
    }

  def validOutgoing(fields: NonEmptyList[(String, String, Boolean)], request: Boolean): Boolean =
    splitFields(
      fields.toList.map(h => (h._1, h._2)),
      if (request) requestPsedo else Set(STATUS),
      request,
    ).isDefined

  private def outgoingFields(
      headers: Headers,
      request: Boolean,
  ): List[(String, String, Boolean)] = {
    val nominated = headers.headers
      .filter(_.name == CIString("connection"))
      .flatMap(_.value.split(',').map(v => CIString(v.trim)))
      .toSet
    headers.headers
      .filterNot(h =>
        connectionHeadersFields(h.name) || nominated(h.name) ||
          h.name == CIString("te") && (!request || !h.value.equalsIgnoreCase("trailers"))
      )
      .map(h =>
        (
          h.name.toString.toLowerCase(java.util.Locale.ROOT),
          h.value,
          Headers.SensitiveHeaders(h.name),
        )
      )
  }

  def requestToHeaders[F[_]](req: Request[F]): NonEmptyList[(String, String, Boolean)] = {
    val p = req.uri.path.renderString
    val query = req.uri.query.renderString
    val path = (if (p.nonEmpty) p
                else if (req.method == Method.OPTIONS && req.uri.query.isEmpty) "*"
                else "/") +
      (if (req.uri.query.isEmpty) "" else "?" + query)
    val authority = req.uri.authority
      .map(_.copy(userInfo = None).renderString)
      .orElse(req.headers.headers.find(_.name == CIString("host")).map(_.value))
    val control =
      if (req.method == Method.CONNECT) Nil
      else List((SCHEME, req.uri.scheme.fold("https")(_.value), false), (PATH, path, false))
    NonEmptyList(
      (METHOD, req.method.toString, false),
      control ++
        authority.toList.map(a => (AUTHORITY, a, false)) ++
        outgoingFields(req.headers, request = true).filterNot(_._1 == "host"),
    )
  }

  def responseToHeaders[F[_]](resp: Response[F]): NonEmptyList[(String, String, Boolean)] =
    NonEmptyList(
      (STATUS, resp.status.code.toString, false),
      outgoingFields(resp.headers, request = false),
    )

  def findWithNoDuplicates[A](values: List[A])(predicate: A => Boolean): Option[A] =
    values.filter(predicate) match {
      case value :: Nil => Some(value)
      case _ => None
    }
}
