# momo-result HTTP/2 patch

This public fork carries an application-maintained patch on top of http4s
`v0.23.37`. It is published as source for reproducible local builds; this
repository does not publish a binary artifact for the patch.

## Fixed source reference

- Upstream base: `v0.23.37`
- Patch branch: `main`
- Original patch commits: `7a01600fc0`, `3935c2a53b`, `02db430fe0`, `2e264bc298`

The patch is not an official http4s release. Consumers should use the fixed
commit SHA, rather than the moving branch. The historical
`momo-h2-rfc-v02336` tag remains the previous patch version.

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
