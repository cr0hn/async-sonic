# async-sonic

A zero-dependency, fully typed asyncio client for the [Sonic](https://github.com/valeriansaliou/sonic) search index, with connection pooling and pipelining.

[![CI](https://github.com/cr0hn/async-sonic/actions/workflows/ci.yml/badge.svg)](https://github.com/cr0hn/async-sonic/actions/workflows/ci.yml)
![Python 3.14+](https://img.shields.io/badge/python-3.14%2B-blue)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

LLM-friendly reference: [`llms.txt`](llms.txt).

## Table of contents

- [Why async-sonic](#why-async-sonic)
- [Installation](#installation)
- [Quickstart](#quickstart)
- [API reference](#api-reference)
- [Concurrency and performance](#concurrency-and-performance)
- [Error handling](#error-handling)
- [Escaping and limits](#escaping-and-limits)
- [Compatibility and limitations](#compatibility-and-limitations)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## Why async-sonic

Sonic is a fast, lightweight search index that speaks a plain-text protocol
([`PROTOCOL.md`](https://github.com/valeriansaliou/sonic/blob/master/PROTOCOL.md)).
The asyncio client that existed on PyPI (`asonic`, last released in 2020) does not work, and
the other clients are synchronous. `async-sonic` implements the protocol from its specification
on top of `asyncio.open_connection` and nothing else.

- **One class**: `Sonic(host, port, password)` used as `async with`, with flat, obviously named
  methods (`push`, `query`, `suggest`, ...). Channels, handshake and pool are hidden.
- **Fast**: a lazy per-channel connection pool plus pipelining (one background reader per
  connection). See [the numbers](#concurrency-and-performance).
- **Zero runtime dependencies**, Python 3.14+, strict typing, `py.typed`.
- **Honest errors**: no hidden retries; every exception message says what happened and what to do.
- **LLM-friendly**: short docstrings with a one-line example on every public method, plus
  [`llms.txt`](llms.txt).

## Installation

```bash
uv add async-sonic        # or: pip install async-sonic
```

The package is not on PyPI yet; until then install from Git:

```bash
uv add git+https://github.com/cr0hn/async-sonic
# or: pip install git+https://github.com/cr0hn/async-sonic
```

## Quickstart

<!-- quickstart -->
```python
import asyncio
from async_sonic import Sonic


async def main() -> None:
    async with Sonic("localhost", 1491, "SecretPassword") as sonic:
        await sonic.push("videos", "catalog", "video:1", "cats and dogs are funny", lang="eng")
        await sonic.push("videos", "catalog", "video:2", "stray dogs", lang="eng")
        print(await sonic.query("videos", "catalog", "cats", lang="eng"))
        await sonic.trigger(
            "consolidate"
        )  # SUGGEST reads a graph that is only updated on consolidate
        print(await sonic.suggest("videos", "catalog", "fun"))
        await sonic.flush_collection("videos")


asyncio.run(main())
```
Output:
```text
['video:1']
['funny']
```
<!-- /quickstart -->

This block is executed as a test against a real Sonic
(`tests/test_integration.py::test_readme_quickstart_runs`), so it cannot drift from the code.
To try it, start Sonic (see [Development](#development)) and use its host, port and password.

## API reference

`Sonic` opens nothing on construction or on `async with` entry: connections are opened lazily on
the first command, one pool per channel.

```python
Sonic(host="localhost", port=1491, password="", *,
      pool_size=4, max_in_flight=None, timeout=10.0, connect_timeout=5.0)
```

| Method | Sonic command | Channel | Returns |
|---|---|---|---|
| `query(collection, bucket, terms, *, limit, offset, lang)` | `QUERY` | search | `list[str]` object ids, best first |
| `suggest(collection, bucket, word, *, limit)` | `SUGGEST` | search | `list[str]` words |
| `list_words(collection, bucket, *, limit, offset)` | `LIST` | search | `list[str]` words |
| `push(collection, bucket, object, text, *, lang)` | `PUSH` | ingest | `None` |
| `pop(collection, bucket, object, text)` | `POP` | ingest | `int` |
| `count(collection, bucket=None, object=None)` | `COUNT` | ingest | `int` |
| `flush_collection(collection)` | `FLUSHC` | ingest | `int` |
| `flush_bucket(collection, bucket)` | `FLUSHB` | ingest | `int` |
| `flush_object(collection, bucket, object)` | `FLUSHO` | ingest | `int` |
| `trigger(action=None, data=None)` | `TRIGGER` | control | `str` |
| `info()` | `INFO` | control | `dict[str, int]` |
| `ping()` | `PING` | control | `None` |
| `help(manual=None)` | `HELP` | control | `str` |

`close()` (or leaving the `async with`) sends `QUIT` on every connection. `quote(text)` is public.

`lang` is an ISO 639-3 code (`"eng"`, `"spa"`) or `"none"`. If omitted, Sonic guesses the
language of the text, and may guess differently at index and query time: pass it on both sides.

Deliberate differences from the protocol:

- `list_words`: Sonic's `LIST` enumerates **words** of the index, not objects.
- `count` uses `COUNT` rather than `COUNTC/COUNTB/COUNTO` (see [Compatibility](#compatibility-and-limitations)).

## Concurrency and performance

A Sonic channel is a TCP connection in one mode (search, ingest or control).

- **Pool**: up to `pool_size` connections per channel, opened when needed (an idle connection is
  reused before a new one is opened). A command goes to the least loaded connection.
- **Pipelining**: each connection writes commands without waiting for replies, and a background
  reader task hands the replies to futures. `PROTOCOL.md` allows this: immediate replies (`OK`,
  `RESULT`, `PONG`, `PENDING <id>`, `ERR`) arrive **in order** (a FIFO of futures), while the
  `EVENT` lines of `QUERY`/`SUGGEST`/`LIST` may arrive **out of order** and are matched by the
  id of their `PENDING`. `max_in_flight` caps simultaneous commands per connection
  (`None` = unlimited, `1` = no pipelining).
- **No retries, no magic reconnection.** If a connection drops, its in-flight commands fail with
  `SonicConnectionError`; the *next* command opens a fresh connection. What to retry, and when,
  is your decision (a `PUSH` is idempotent if you `flush_object` first; a `POP` is not).
- **Per-command timeout** (`timeout`) raises `SonicTimeout`, but the connection stays usable: the
  late reply is discarded when it arrives and nothing gets out of sync.

### Measured numbers

`benchmarks/bench.py`: 2000 operations per row, Sonic v1.9.1 in Docker Desktop (4 CPUs) on an
Apple M2 Max, over localhost. **It is a single run and numbers vary from run to run**: trust the
orders of magnitude, not the decimals.

| Scenario | QUERY ops/s | PUSH ops/s |
|---|---:|---:|
| 1 connection, sequential | 2205 | 3061 |
| pool of 8, concurrent, no pipelining | 10549 | 7559 |
| 1 connection, pipelining | 13464 | 12795 |
| pool of 8 + pipelining | 14843 | 11104 |

Pipelining is the big lever: 4x to 6x over sequential. On top of it the pool did not help in
this run (an earlier run showed +40% for queries and nothing for writes), so with pipelining one
connection is often enough; the pool mostly matters when `max_in_flight` is capped. Over a real
network with latency the gap to the sequential case is larger, because a round trip is paid once
per burst instead of once per command; that was not measured.

## Error handling

Every exception inherits from `SonicError`; messages say what happened and what to do.

| Exception | When |
|---|---|
| `SonicConnectionError` | could not connect, or the connection dropped, was closed or received `ENDED` |
| `SonicTimeout` (subclass of the previous one) | `connect_timeout` or `timeout` expired |
| `SonicServerError` | Sonic answered `ERR ...` (`.code`, `.line`); also a wrong password (`authentication_failed`) |
| `SonicProtocolError` | Sonic said something outside `PROTOCOL.md`; the connection is closed |
| `ValueError` (builtin) | invalid argument, or a command that does not fit in the buffer; nothing is sent |

```python
from async_sonic import Sonic, SonicServerError, SonicTimeout

async with Sonic(password="SecretPassword") as sonic:
    try:
        await sonic.query("videos", "catalog", "cats", limit=0)
    except SonicServerError as exc:
        print(exc.code)  # policy_reject
    except SonicTimeout:
        ...  # Sonic did not answer in time; the connection is still usable
```

## Escaping and limits

`quote(text)` wraps text in quotes: `"` becomes `\"`, `\` becomes `\\` (so a trailing backslash
cannot swallow the closing quote) and newlines (`\n`, `\r`) become a space, because a raw newline
would cut the command in two; Sonic tokenizes on whitespace, so no word is lost. Unicode goes
through as UTF-8. `collection`, `bucket`, `object`, `action`... cannot be empty or contain
whitespace, quotes or control characters (`ValueError`).

**Buffer.** `STARTED ... buffer(N)` (20000 by default) is the limit of a whole command line,
newline included. `PROTOCOL.md` asks clients to split: `push` does it for you, emitting several
`PUSH` commands for the same object, always cutting **between words**. `query`, `suggest` and
`pop` are not split (splitting would change their meaning) and raise `ValueError`, as does a
single word longer than the buffer.

## Compatibility and limitations

- **Verified**: Sonic **v1.9.1** (official image `valeriansaliou/sonic:v1.9.1`) on Docker
  Desktop, macOS arm64, Python 3.14. Every command in the table above; text with quotes,
  backslashes, newlines, accents, CJK and emoji; a 48 KB text (split by the buffer); real `ERR`
  replies; a wrong password; and 200 concurrent queries over the pool with pipelining.
- **Deviations from `PROTOCOL.md` found on v1.9.1**:
  - `COUNTC`, `COUNTB` and `COUNTO` answer `ERR unknown_command`; `COUNT` is used instead, and
    `count(collection, bucket)` returns the number of **distinct words**, not of objects
    (measured: 2 objects, 4 words, `count` returned 4).
  - `SUGGEST` and `LIST` only see new words after `trigger("consolidate")`.
  - A wrong password is answered with `ENDED authentication_failed` (surfaced as `SonicServerError`).
  - Replies end in `\r\n`.
- **Not verified**: other Sonic versions (the `COUNTC/B/O` commands may exist in later ones);
  `TRIGGER backup` and `restore` (they are sent, never tested against real data); Linux and
  Windows (CI covers Linux); sustained load or a server with many clients; network latency.
- No TLS: Sonic Channel is plain TCP. Do not expose it to an untrusted network.
- Searching without accents (`cancion` for `canción`) depends on the server configuration
  (`diacritic_folding_enabled`), not on this client; the test `sonic.cfg` does not enable it.

## Development

```bash
uv sync
uv run ruff check . && uv run ruff format --check .
uv run pyright
uv run pytest                      # everything
uv run pytest tests/test_unit.py   # unit tests only, no docker needed
uv run python benchmarks/bench.py  # benchmark against a real Sonic
```

- `tests/test_unit.py` runs against a fake asyncio server (`tests/fake_sonic.py`) that speaks the
  real protocol: handshake, out-of-order `PENDING`/`EVENT`, `ERR`, abrupt close, `ENDED`, slow
  replies (timeouts and late replies), buffer splitting, pool and pipelining with latency.
- `tests/test_integration.py` runs against a **real Sonic**. If `docker` is available, the
  fixture starts `valeriansaliou/sonic:v1.9.1` with `tests/sonic.cfg` (password `SecretPassword`)
  on a free port. **Without docker these tests are skipped with an explicit reason**
  (`SKIPPED: docker is not available...`): always check the `skipped` count, because a green run
  with every integration test skipped does not prove the real wiring. CI fails if any test is skipped.

## Contributing

Issues and pull requests are welcome; see [CONTRIBUTING.md](CONTRIBUTING.md). Please follow the
[Code of Conduct](CODE_OF_CONDUCT.md). Security reports: [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE) (c) 2026 Daniel Alfocea.
