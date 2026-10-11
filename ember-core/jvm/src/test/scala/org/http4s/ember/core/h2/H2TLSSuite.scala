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

import fs2.io.net.tls.TLSParameters
import org.http4s.Http4sSuite

class H2TLSSuite extends Http4sSuite {
  test("HTTP/2 TLS parameters exclude TLS versions before 1.2") {
    val parameters =
      H2TLS.transform(TLSParameters(protocols = Some(List("TLSv1", "TLSv1.1", "TLSv1.2"))))
    assertEquals(parameters.protocols, Some(List("TLSv1.2")))
    val defaults = H2TLS.transform(TLSParameters()).protocols.get
    assert(defaults.contains("TLSv1.2"))
    assert(defaults.forall(Set("TLSv1.2", "TLSv1.3")))
  }

  test("ALPN selects only a protocol that the peer offered") {
    val select = H2TLS.transform(TLSParameters()).handshakeApplicationProtocolSelector.get
    assertEquals(select(null, List("h2", "http/1.1")), "h2")
    assertEquals(select(null, List("http/1.1")), "http/1.1")
    assertEquals(select(null, List("unknown")), "")
  }
  test("HTTP/2 excludes static key exchange and CBC cipher suites") {
    val allowed = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
    val params = TLSParameters(cipherSuites =
      Some(List("TLS_RSA_WITH_AES_128_GCM_SHA256", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", allowed))
    )
    assertEquals(H2TLS.transform(params).cipherSuites, Some(List(allowed)))
    assert(H2TLS.transform(TLSParameters()).cipherSuites.exists(_.contains(allowed)))
  }

}
