# Contributing

Thank you for your interest in ushr. Read [DESIGN.md](DESIGN.md) before a large
change. Open an issue first to agree on the approach.

## Build and test

You need the Go version in `go.mod`.

```bash
make build   # bin/ushr, bin/ushr-controller, bin/ushr-agent
make test    # go test ./...
make lint    # golangci-lint run
```

CI runs `go build ./...`, `go test -race ./...` and golangci-lint.
Run all three before you open a pull request.

The Tart driver smoke test needs a real Tart host and a GitHub org with the
App installed. It creates real runner registrations. Do not run it against a
production org. See the README.

## Pull requests

- Keep each pull request to one change.
- Add or extend tests in the package that you change.
- Use [Conventional Commits](https://www.conventionalcommits.org/):
  `feat:`, `fix:`, `docs:`, `chore:`. Keep the first line under 72 characters.
  The body says why the change is necessary.
- Do not commit credentials, GitHub App keys or `agent.yaml` files.

## Security issues

Do not report vulnerabilities in issues or pull requests.
See [SECURITY.md](SECURITY.md).

## License

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE).
