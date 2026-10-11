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

import cats.syntax.all._
import fs2.io.net.tls.TLSParameters

import javax.net.ssl.SSLEngine
import javax.net.ssl.SSLContext

private[h2] abstract class H2TLSPlatform {

  private lazy val defaults = SSLContext.getDefault.getDefaultSSLParameters
  private lazy val protocols = SSLContext.getDefault.getSupportedSSLParameters.getProtocols.toSet

  // RFC 9113 Appendix A excludes static key exchange and non-AEAD TLS 1.2 suites.
  private def permittedCipher(name: String): Boolean =
    name.startsWith("TLS_AES_") || name.startsWith("TLS_CHACHA20_") ||
      ((name.startsWith("TLS_ECDHE_") || name.startsWith("TLS_DHE_")) &&
        (name.contains("_GCM_") || name.contains("_CHACHA20_POLY1305_")))

  def transform(params: TLSParameters): TLSParameters =
    TLSParameters(
      algorithmConstraints = params.algorithmConstraints,
      applicationProtocols = List("http/1.1", "h2").some,
      cipherSuites =
        params.cipherSuites.getOrElse(defaults.getCipherSuites.toList).filter(permittedCipher).some,
      enableRetransmissions = params.enableRetransmissions,
      endpointIdentificationAlgorithm = params.endpointIdentificationAlgorithm,
      maximumPacketSize = params.maximumPacketSize,
      protocols = params.protocols
        .getOrElse(List("TLSv1.3", "TLSv1.2").filter(protocols))
        .filter(p => p == "TLSv1.3" || p == "TLSv1.2")
        .some,
      serverNames = params.serverNames,
      sniMatchers = params.sniMatchers,
      useCipherSuitesOrder = params.useCipherSuitesOrder,
      needClientAuth = params.needClientAuth,
      wantClientAuth = params.wantClientAuth,
      handshakeApplicationProtocolSelector = { (_: SSLEngine, l: List[String]) =>
        l.find(_ === "h2").orElse(l.find(_ === "http/1.1")).getOrElse("")
      }.some,
    )
}
