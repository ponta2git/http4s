# momo-result HTTP/2 patch

This public fork carries an application-maintained patch on top of http4s
`v0.23.37`. It is published as source for reproducible local builds; this
repository does not publish a binary artifact for the patch.

## Fixed source reference

- Upstream base: `v0.23.37`
- Patch branch: `master`
- Original patch commits: `7a01600fc0`, `3935c2a53b`, `02db430fe0`, `2e264bc298`

The patch is not an official http4s release. Consumers should use the fixed
commit SHA, rather than the moving branch. The historical
`momo-h2-rfc-v02336` tag remains the previous patch version.

Fork versions include the upstream base and source commit:
`0.23.37-momo-<commit-prefix>-SNAPSHOT`. Clean clones produce the same version
without mirroring upstream release tags or triggering their release workflows.
Dirty checkouts include a timestamp and must not be used for pinned builds.

The patch preserves late closed-stream frame handling, HPACK synchronization,
stream state validation, and connection/stream flow control. It includes the
upstream 0.23.37 frame/header size limits and HTTP/2 idle/stall timeouts.
Cleartext HTTP/2 uses prior knowledge; HTTP/1.1 h2c Upgrade is no longer supported,
matching upstream 0.23.37.

Stream transitions use the current state after header decoding. Invalid final
headers or a content-length mismatch reset the stream before normal completion,
and concurrent local completion or reset cannot reopen it. Empty DATA can finish
a stream even after SETTINGS reduces its send window below zero. Connection
credit is reserved before a socket write and is independent of stream window
settings.

After sending a complete response, the server closes unconsumed request input.
If the client has not ended the request, the server sends `RST_STREAM(NO_ERROR)`
after the response's `END_STREAM`, as permitted by RFC 9113 section 8.1. Closing
input releases body readers; stream resets also complete without waiting for a
body consumer. Receive credit now bounds retained body bytes, and credit is
replenished when the application consumes them. Empty DATA does not occupy the
body buffer. A client retains a
completed response when it receives the subsequent `NO_ERROR` reset.

## Reproduce from source

```bash
git clone https://github.com/ponta2git/http4s.git
cd http4s
git checkout --detach <pinned-commit-sha>
sbt '++3.3.6' ember-core/test server/test ember-server/test tests/test
sbt '++3.3.6' core/mimaReportBinaryIssues server/mimaReportBinaryIssues \
  ember-core/mimaReportBinaryIssues ember-server/mimaReportBinaryIssues
```

Build the required artifact locally from this checkout and use that artifact
in the application. Keep the upstream LICENSE and NOTICE files with any
distributed copy.

## RFC review (2026-10-11)

The implementation reviewed here is Ember HTTP/2. The repository has an
`HTTP/3` version constant, but no HTTP/3 transport, QUIC, or QPACK implementation.
[RFC 9114](https://www.rfc-editor.org/rfc/rfc9114.html) therefore supplies context
for shared HTTP semantics, not an HTTP/3 conformance claim. The version constant's
reference now points to that final RFC.

The changes address the following concrete defects against
[RFC 9113](https://www.rfc-editor.org/rfc/rfc9113.html) and
[RFC 7541](https://www.rfc-editor.org/rfc/rfc7541.html):

| Area | Defect and correction | RFC sections |
| --- | --- | --- |
| Frame parsing | Truncated fixed fields could throw; signed padding and dependency octets were misread. Check sizes before indexing, decode unsigned octets, and reject invalid stream IDs and padding. | 9113 §§4, 6 |
| Connection preface | A partial socket read was treated as an invalid preface. Accumulate matching bytes under one timeout; require the peer's first frame to be non-ACK SETTINGS. Send the client's SETTINGS before starting its reader. | 9113 §3.4 |
| Settings | Correct the maximum initial window to 2^31−1, apply repeated settings in wire order, reject a server enabling push, advertise the local concurrency limit, and apply the peer's HPACK table limit. | 9113 §§6.5, 6.9.2; 7541 §4.2 |
| Header fields | Validate names, values, pseudo-header order/uniqueness/context, all TE occurrences, and Content-Length syntax. Reject CR/LF/NUL and ambiguous framing; remove Connection-nominated outgoing fields. | 9113 §§8.1.1, 8.2, 8.3 |
| Request target | Parse authority as authority rather than a relative URI; reject userinfo and mismatching Host; validate path and scheme; implement ordinary CONNECT's pseudo-header shape. Combine split Cookie fields before application delivery. | 9113 §§8.2.3, 8.3.1, 8.5 |
| Response sequence | Keep informational responses separate from the final response; reject 101 and terminal 1xx; reject DATA before final response headers, invalid trailers, and trailers without END_STREAM. | 9113 §§8.1, 8.3.2 |
| Message length | Check Content-Length on terminal HEADERS as well as DATA, and reject excess bytes before END_STREAM. HEAD/204/304 metadata is not mistaken for a required body; successful CONNECT switches to tunnel framing. Suppress server response bodies where content is forbidden. | 9113 §§8.1.1, 8.5 |
| HPACK | Empty field sections are legal trailers. Detect truncated representations and table updates after fields before invoking the incremental decoder; emit pending minimum/final table-size updates before subsequent encoded fields. | 7541 §§4.2, 5, 6 |
| Header fragmentation | Split outbound HEADERS and PUSH_PROMISE using the peer frame limit, preserve END_STREAM on HEADERS, and enqueue each complete CONTINUATION sequence together under the encoder lock. | 9113 §§4.3, 6.2, 6.6, 6.10 |
| Flow control | Reserve stream credit atomically before enqueueing DATA; split DATA to available connection credit. Replenish stream credit on consumption and route connection control frames independently of blocked DATA. Socket reads no longer depend on a possibly zero stream window. | 9113 §§5.2, 6.9 |
| Stream lifecycle | Enforce remote stream concurrency, reject unreserved server streams and disabled push, stop local stream creation after GOAWAY or ID exhaustion, and release waiters/watchdogs on connection termination. Closed cached connections are removed only if still owned by that cache entry. | 9113 §§5.1, 5.4, 6.8 |
| Push | Reject unsafe/body-bearing promised requests and promises outside the connection's origin before invoking the client callback. Release failed push entries and respect the peer's concurrency when producing pushes. | 9113 §8.4 |
| Resource limits | Bound empty CONTINUATION sequences by frame count, enforce compressed-block limits on unfragmented blocks too, and keep decoded header limits independent of compressed size. | 9113 §§10.5, 10.5.1 |
| TLS (JVM) | Restrict protocols to supported TLS 1.2/1.3 versions, exclude static-key-exchange/non-AEAD TLS 1.2 suites, and choose only ALPN protocols actually offered by the peer. Preserve the caller's authentication and endpoint-validation parameters. | 9113 §§3.2, 9.2 |

Regression coverage includes short payloads across all known frame types and all
256 flag values, padding boundaries 0/127/128/255, malformed field sections,
HPACK truncation and table updates, interim responses, trailers, HEAD metadata,
flow-control races, stalled DATA with live control frames, preface fragmentation,
connection EOF, and continuation flooding. Existing tests with malformed response
fields or assumptions about cross-stream completion order were corrected.

Validation: Scala 3.3.6 JVM Ember core/server/client passed 243/39/34 tests
respectively (two existing core tests remain ignored). MiMa passed for all three
modules. Additional checks during the review passed: 175 HTTP/2 tests on Scala
2.13.18, 53 frame/header/HPACK tests on Scala.js, and compilation on Scala 2.12.21
and Scala Native. The final JVM checks are reproducible with:

```bash
sbt '++3.3.6' 'ember-core/test' 'ember-server/test' 'ember-client/test' \
  'ember-core/mimaReportBinaryIssues' 'ember-server/mimaReportBinaryIssues' \
  'ember-client/mimaReportBinaryIssues'
```

These deterministic regression tests do not constitute a formal proof of
complete RFC conformance. No external h2spec run or live HTTP/3 interoperability
test was performed. JVM TLS parameter tests do not establish all
provider-specific cipher, renegotiation, or native/JavaScript TLS behavior.

## Lifecycle handoff (2026-10-11)

The momo-result handoff reproduced three failures against `0ff0cbb2f57a`:
RST_STREAM did not cancel an application suspended outside request-body reads;
server shutdown did not send GOAWAY during its grace period; and an HTTP/1 HEAD
response with unknown representation length acquired `Content-Length: 0`.

- Stream cancellation now owns the handler, admission permit, response resource,
  and any in-progress pushes. Expected peer resets are debug-level cancellation,
  not application errors. Application errors retain their normal error reporting.
- Server shutdown freezes the last accepted stream ID and writes NO_ERROR GOAWAY
  before draining. New requests are refused while their HPACK blocks are consumed.
  Accepted handlers and response writes finish before connection release; the
  configured shutdown timeout still bounds unfinished work.
- HEAD encoding preserves supplied representation metadata and emits no body,
  chunk terminator, or trailers. It does not infer a representation length from
  an empty wire body. Suppressed response resources are finalized, and the next
  response on a persistent connection starts immediately after HEAD's headers.
- Remote-stream error resets use the control queue, while local stream resets
  remain ordered after their introducing HEADERS/PUSH_PROMISE. NO_ERROR following
  an early response remains ordered after END_STREAM. Competing queue reads retain
  both completed results, avoiding silent frame loss during simultaneous arrivals.
- Reset DATA is discarded even after the handler's stream-map entry is removed.
  Cancellation metadata is retained only for outstanding DATA offers and writes,
  then released as frames are sent or discarded. Reset notification wakes a writer
  waiting for connection credit, so a canceled body cannot spend new credit or
  block a later response. Normal early-response NO_ERROR retains its queued body.

The shutdown behavior follows [RFC 9113 §6.8](https://www.rfc-editor.org/rfc/rfc9113.html#section-6.8).
HEAD metadata and framing follow [RFC 9110 §8.6](https://www.rfc-editor.org/rfc/rfc9110.html#section-8.6)
and [RFC 9112 §6.3](https://www.rfc-editor.org/rfc/rfc9112.html#section-6.3).

Validation of this handoff:

- JVM Scala 3.3.6: Ember core/server/client passed 256/48/34 tests (two existing
  ignored tests); all three modules passed MiMa. The affected suites passed again
  after the final shutdown/push adjustment.
- Scala.js: 109 connection/stream tests passed. Scala 2.13.18: 123
  connection/stream/server-body tests passed. Scala Native Ember core and Scala
  2.12.21 Ember core/server compiled successfully.
- The original momo-result `run.sh` failed all three probes against `0ff0cbb2f57a`
  and passed all three against locally published revised core/server/Ember
  artifacts. The corrected run no longer logged the pre-response reset error.

For the supplied handoff scripts, locally publish `core`, `server`, `ember-core`,
and `ember-server` with Scala 3.3.6, then pass the generated fork version as the
argument to `/private/tmp/momo-http2-handoff/run.sh`. Validation used the temporary
version `0.23.37-momo-handoff-validation-SNAPSHOT`; applications should pin the
clean commit-derived version described above instead.

## SETTINGS ACK follow-up (2026-10-11)

The additional momo-result probe reproduced a SETTINGS ACK waiting behind DATA
at an exhausted connection window, although PING still received its ACK.
SETTINGS acknowledgements now use the control queue, including while DATA waits
for either connection or stream credit (RFC 9113 §6.5.3).

The connection writer also owns deferred HPACK encoding. Table-size changes and
their ACK are one ordered control operation; queued field sections are encoded
and fragmented when transmitted, preserving the size-update boundary required
by RFC 9113 §4.3.1 and RFC 7541 §4.2. Canceling a header offer before admission
therefore cannot leave the encoder ahead of its peer. A disabled, unsent push
is suppressed without mutating HPACK or sending a reset for an unknown stream.

DATA reservations distinguish queued bytes from bytes actually transmitted.
After an ACK, queued DATA obeys a reduced stream window and maximum frame size;
stream WINDOW_UPDATE still reaches queued terminal DATA after handler cleanup.
Window-overflow checks include reserved but unsent bytes. Deferred empty work
cannot complete another operation's write barrier during graceful shutdown.

Validation of the follow-up:

- JVM Scala 3.3.6: Ember core/server/client passed 263/53/34 tests (two existing
  ignored tests); MiMa passed for all three modules. The affected connection,
  stream, and server lifecycle suites passed again after the final accounting
  adjustments.
- Scala.js and Scala 2.13.18 each passed 116 connection/stream tests during this
  follow-up.
- `run-settings-ack.sh` failed against `f1918359ab80` and passed against the revised
  artifacts. The original `run.sh` also passed all three lifecycle/HEAD probes.
  The scripts accept the clean commit-derived version as their first argument;
  the temporary validation version was `0.23.37-momo-settings-ack-validation-SNAPSHOT`.
