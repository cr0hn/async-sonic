# Contributing to async-sonic

Thanks for your interest. Bug reports, fixes and improvements are welcome.

## Ground rules

- **Zero runtime dependencies.** The client is the Sonic protocol on top of `asyncio`; keep it that way.
- **Keep it small.** The library is a single module on purpose. Prefer the simplest change that works.
- **Tests at both levels.** Every change comes with a unit test (fake server, `tests/test_unit.py`) and,
  when it touches behaviour visible to a real server, an integration test (`tests/test_integration.py`).
- **English** for code, comments, docs, error messages and commit messages.
- Protocol questions are settled by Sonic's [`PROTOCOL.md`](https://github.com/valeriansaliou/sonic/blob/master/PROTOCOL.md)
  and by what a real server does; when they disagree, document the deviation in the README.

## Setup

Python 3.14+ and [uv](https://docs.astral.sh/uv/). Integration tests also need Docker.

```bash
git clone https://github.com/cr0hn/async-sonic
cd async-sonic
uv sync
```

## Before opening a pull request

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

CI runs the same commands against a real Sonic in Docker and **fails if any test is skipped**, so
run the whole suite with Docker available and check that the `skipped` count is 0.

If you change the public API, update `README.md` and `llms.txt` in the same pull request.

## Commits

Use [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`, `test:`,
`chore:`...). There is no `CHANGELOG.md`: the git history is the changelog.

## Reporting bugs and security issues

Use the issue templates. For vulnerabilities, follow [SECURITY.md](SECURITY.md) instead of opening a
public issue.

By contributing you agree that your contributions are licensed under the [MIT License](LICENSE).
