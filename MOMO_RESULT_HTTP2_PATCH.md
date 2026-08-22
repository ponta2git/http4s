# momo-result HTTP/2 patch

This public fork carries an application-maintained patch on top of http4s
`v0.23.36`. It is published as source for reproducible local builds; this
repository does not publish a binary artifact for the patch.

## Fixed source reference

- Upstream base: `v0.23.36`
- Patch branch: `momo/h2-rfc-v02336`
- Patch tag: `momo-h2-rfc-v02336`
- Patch commits: `7a01600fc0`, `3935c2a53b`, `02db430fe0`, `2e264bc298`

The patch is not an official http4s release. Consumers should use the fixed
patch tag (or its commit SHA), rather than the fork's default branch.

## Reproduce from source

```bash
git clone --branch momo-h2-rfc-v02336 https://github.com/ponta2git/http4s.git
cd http4s
sbt ember-core/Test/compile
```

Build the required artifact locally from this checkout and use that artifact
in the application. Keep the upstream LICENSE and NOTICE files with any
distributed copy.
