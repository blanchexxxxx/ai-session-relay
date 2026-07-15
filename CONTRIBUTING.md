# Contributing

This repository is an automated, sanitized export of a production continuity engine. Pull requests and issues are welcome, but generated files have an upstream source of truth.

Before a maintainer merges a public contribution, the same change must be accepted into the upstream sanitized mirror. The next export would otherwise overwrite a public-only edit. The maintainer will either port the contribution upstream and merge it here, or explain why it cannot be included.

Every automated export runs the full public test suite and a private-material leak guard before it opens a pull request. Public CI must pass before the export workflow merges that pull request.
