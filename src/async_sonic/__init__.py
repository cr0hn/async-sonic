"""Asyncio client for Sonic (https://github.com/valeriansaliou/sonic), zero dependencies.

Usage: ``async with Sonic("localhost", 1491, "password") as sonic: await sonic.query(...)``.
Full documentation in README.md and llms.txt.
"""

from __future__ import annotations

import asyncio
import re
from collections import deque
from typing import Self

__all__ = [
    "Sonic",
    "SonicConnectionError",
    "SonicError",
    "SonicProtocolError",
    "SonicServerError",
    "SonicTimeout",
    "quote",
]


class SonicError(Exception):
    """Base class of every error raised by this library."""


class SonicConnectionError(SonicError):
    """Could not connect, or the connection dropped or is closed.

    In-flight commands on that connection fail with this; the next command opens a new one.
    """


class SonicTimeout(SonicConnectionError):
    """`connect_timeout` (while connecting) or `timeout` (per command) expired."""


class SonicServerError(SonicError):
    """Sonic answered `ERR <code>(<detail>)`. `.code` is the code, `.line` the whole line."""

    def __init__(self, line: str) -> None:
        match = re.match(r"ERR (\w+)", line)
        self.line = line
        self.code = match.group(1) if match else ""
        hint = _HINTS.get(self.code, "")
        super().__init__(f"Sonic rejected the command: {line}" + (f". {hint}" if hint else ""))


class SonicProtocolError(SonicError):
    """Sonic said something PROTOCOL.md does not cover (incompatible version?). The connection is closed."""


_HINTS = {
    "authentication_failed": "Wrong password: use `channel.auth_password` from sonic.cfg.",
    "invalid_format": "Invalid command format: if you only use the public API this is an "
    "async-sonic bug; please open an issue with the text you sent.",
    "policy_reject": "Value outside the server limits (limit/offset/text): check "
    "`[search]` and `[store]` in sonic.cfg.",
    "unknown_command": "This Sonic version does not know that command.",
    "not_found": "Sonic does not know that resource or action (e.g. trigger: consolidate, backup, restore).",
}
_BUFFER = re.compile(r"buffer\((\d+)\)")
_KV = re.compile(r"(\w+)\((-?\d+)\)")


def quote(text: str) -> str:
    """Quote text for PUSH/POP/QUERY/SUGGEST. E.g. ``quote('say "hi"')``.

    PROTOCOL.md only requires `\\"` for quotes; the backslash is doubled so that a trailing
    backslash cannot swallow the closing quote. Newlines become spaces: a raw newline would
    end the command halfway (Sonic tokenizes on spaces, so no word is lost).
    """
    flat = text.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    return '"' + flat.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _token(value: str, what: str) -> str:
    if not value or any(c.isspace() or c == '"' or not c.isprintable() for c in value):
        raise ValueError(
            f"Invalid {what} {value!r}: it cannot be empty or contain whitespace, quotes or "
            "control characters. Use a short name such as 'videos' or 'video:42'."
        )
    return value


def _opts(*, limit: int | None = None, offset: int | None = None, lang: str | None = None) -> str:
    out = ""
    if limit is not None:
        out += f" LIMIT({int(limit)})"
    if offset is not None:
        out += f" OFFSET({int(offset)})"
    if lang is not None:
        if not lang.isalpha():
            raise ValueError(f"Invalid lang {lang!r}: use an ISO 639-3 code ('eng') or 'none'.")
        out += f" LANG({lang})"
    return out


def _settle(fut: asyncio.Future[str], value: str | Exception) -> None:
    if not fut.done():
        if isinstance(value, Exception):
            fut.set_exception(value)
        else:
            fut.set_result(value)


class _Conn:
    """One TCP connection in one mode, with a background reader.

    Pipelining: every command is written without waiting. Immediate replies (OK, RESULT,
    PONG, PENDING <id>, ERR) arrive in order, so a FIFO of futures matches them. The `EVENT`
    lines of QUERY/SUGGEST/LIST (PROTOCOL.md: they may arrive out of order) are matched by id.
    A command whose timeout expires leaves its slot in the queue/table: the late reply is
    discarded on arrival, so the connection never gets out of sync.
    """

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader, self._writer = reader, writer
        self.buffer = 0
        self.load = 0  # active calls, used to pick a connection
        self.timeout = 10.0
        self.gate: asyncio.Semaphore | None = None
        self._fifo: deque[tuple[asyncio.Future[str], str | None]] = deque()
        self._events: dict[str, tuple[asyncio.Future[str], str]] = {}
        self._task: asyncio.Task[None] | None = None
        self.closed = False

    @classmethod
    async def open(
        cls,
        host: str,
        port: int,
        password: str,
        mode: str,
        *,
        connect_timeout: float,
        timeout: float,
        max_in_flight: int | None,
    ) -> _Conn:
        where = f"{host}:{port}"
        try:
            async with asyncio.timeout(connect_timeout):
                reader, writer = await asyncio.open_connection(host, port, limit=1 << 20)
                conn = cls(reader, writer)
                try:
                    greeting = await conn._readline()
                    if not greeting.startswith("CONNECTED "):
                        raise SonicProtocolError(
                            f"{where} does not speak the Sonic protocol (greeting {greeting!r}): "
                            "check that the port is the Sonic Channel one (1491 by default)."
                        )
                    writer.write(f"START {mode} {password}\n".encode())
                    started = await conn._readline()
                    match = _BUFFER.search(started)
                    if not started.startswith(f"STARTED {mode} ") or match is None:
                        raise SonicProtocolError(f"Unexpected reply to START: {started!r}")
                    conn.buffer = int(match.group(1))
                except BaseException:
                    writer.close()
                    raise
        except TimeoutError:
            raise SonicTimeout(
                f"Timeout ({connect_timeout}s) connecting to Sonic at {where}: check "
                "host/port or raise `connect_timeout`."
            ) from None
        except OSError as exc:
            raise SonicConnectionError(
                f"Could not connect to Sonic at {where} ({exc}): check that Sonic is "
                "running and that host and port are correct."
            ) from exc
        conn.timeout = timeout
        conn.gate = asyncio.Semaphore(max_in_flight) if max_in_flight else None
        conn._task = asyncio.create_task(conn._run())
        return conn

    async def _readline(self) -> str:
        try:
            raw = await self._reader.readline()
        except ValueError as exc:  # longer than `limit`
            raise SonicProtocolError("Sonic sent a line longer than 1 MiB") from exc
        except OSError as exc:
            raise SonicConnectionError(f"Lost the connection to Sonic ({exc}).") from exc
        if not raw.endswith(b"\n"):
            raise SonicConnectionError(
                "Sonic closed the connection (restart, `tcp_timeout` or crash): the next "
                "command will open a new connection."
            )
        line = raw.decode(errors="replace").rstrip("\r\n")
        if line.startswith("ENDED authentication_failed") and self._task is None:
            raise SonicServerError("ERR authentication_failed")  # this is how Sonic answers START
        if line.startswith("ENDED "):
            raise SonicConnectionError(f"Sonic ended the session ({line}).")
        if line.startswith("ERR ") and self._task is None:  # during the handshake
            raise SonicServerError(line)
        return line

    async def _run(self) -> None:
        exc: Exception
        try:
            while True:
                self._dispatch(await self._readline())
        except SonicError as e:
            exc = e
        except Exception as e:  # ponytail: anything unexpected also closes the connection
            exc = SonicProtocolError(f"Internal error while reading from Sonic: {e!r}")
        self._shutdown(exc)

    def _dispatch(self, line: str) -> None:
        if line.startswith("EVENT "):
            parts = line.split(" ", 3)
            entry = self._events.pop(parts[2], None) if len(parts) > 2 else None
            if entry is None:
                raise SonicProtocolError(f"EVENT without a previous PENDING: {line!r}")
            fut, name = entry
            if parts[1] != name:
                _settle(fut, SonicProtocolError(f"Expected EVENT {name}, got {line!r}"))
            else:
                _settle(fut, parts[3] if len(parts) > 3 else "")
            return
        if not self._fifo:
            raise SonicProtocolError(f"Reply with no pending command: {line!r}")
        fut, event = self._fifo.popleft()
        if line.startswith("ERR "):
            _settle(fut, SonicServerError(line))
        elif event is None and not line.startswith("PENDING "):
            _settle(fut, line)
        elif event is not None and line.startswith("PENDING "):
            self._events[line.removeprefix("PENDING ").strip()] = (fut, event)
        else:
            _settle(fut, SonicProtocolError(f"Unexpected reply: {line!r}"))

    def _shutdown(self, exc: Exception) -> None:
        self.closed = True
        self._writer.close()
        pending = [f for f, _ in self._fifo] + [f for f, _ in self._events.values()]
        self._fifo.clear()
        self._events.clear()
        for fut in pending:
            _settle(fut, exc)

    async def call(self, line: str, event: str | None = None) -> str:
        """One command. With `event`, returns what follows `EVENT <event> <id>`; without it, the
        first reply line."""
        if self.closed:
            raise SonicConnectionError(
                "Connection closed: open a new `async with Sonic(...)`, or call again "
                "(the pool opens a new connection)."
            )
        data = line.encode() + b"\n"
        if len(data) > self.buffer:
            raise ValueError(
                f"Command of {len(data)} bytes; the buffer Sonic announces is {self.buffer}. "
                "Shorten the text (PUSH splits it on its own; QUERY/SUGGEST/POP do not)."
            )
        self.load += 1
        try:
            if self.gate is not None:
                await self.gate.acquire()
            try:
                fut: asyncio.Future[str] = asyncio.get_running_loop().create_future()
                self._fifo.append(
                    (fut, event)
                )  # queue and write with no await in between: same order
                self._writer.write(data)
                try:
                    async with asyncio.timeout(self.timeout):
                        await self._writer.drain()
                        return await fut
                except TimeoutError:
                    raise SonicTimeout(
                        f"Sonic did not answer {line[:40]!r} within {self.timeout}s: raise `timeout=` "
                        "or check the server load. The connection is still usable."
                    ) from None
                except OSError as exc:
                    raise SonicConnectionError(f"Lost the connection to Sonic ({exc}).") from exc
                finally:
                    fut.cancel()
            finally:
                if self.gate is not None:
                    self.gate.release()
        finally:
            self.load -= 1

    async def close(self, timeout: float) -> None:
        if self.closed or self._task is None:
            return
        self._writer.write(b"QUIT\n")
        try:
            async with asyncio.timeout(timeout):
                await self._task  # the server answers ENDED and closes
        except TimeoutError, OSError:
            pass
        finally:
            self._task.cancel()
            self._shutdown(SonicConnectionError("Connection closed by the client."))


class _Pool:
    """Up to `pool_size` connections of one mode, opened on demand. Picks the least loaded."""

    def __init__(self, mode: str, sonic: Sonic) -> None:
        self.mode, self.sonic = mode, sonic
        self.conns: list[_Conn] = []
        self._lock = asyncio.Lock()

    def _best(self) -> _Conn | None:
        self.conns = [c for c in self.conns if not c.closed]
        best = min(self.conns, key=lambda c: c.load, default=None)
        if best is not None and (best.load == 0 or len(self.conns) >= self.sonic.pool_size):
            return best
        return None

    async def get(self) -> _Conn:
        if (conn := self._best()) is not None:
            return conn
        async with self._lock:
            if (conn := self._best()) is not None:
                return conn
            s = self.sonic
            conn = await _Conn.open(
                s.host,
                s.port,
                s.password,
                self.mode,
                connect_timeout=s.connect_timeout,
                timeout=s.timeout,
                max_in_flight=s.max_in_flight,
            )
            self.conns.append(conn)
            return conn

    async def call(self, line: str, event: str | None = None) -> str:
        return await (await self.get()).call(line, event)

    async def close(self) -> None:
        await asyncio.gather(*(c.close(self.sonic.connect_timeout) for c in self.conns))
        self.conns.clear()


def _ok(reply: str) -> None:
    if reply != "OK":
        raise SonicProtocolError(f"Expected OK, got {reply!r}")


def _result(reply: str) -> str:
    if not reply.startswith("RESULT "):
        raise SonicProtocolError(f"Expected RESULT, got {reply!r}")
    return reply.removeprefix("RESULT ")


def _int(reply: str) -> int:
    text = _result(reply)
    if not text.isdecimal():
        raise SonicProtocolError(f"Expected an integer, got {reply!r}")
    return int(text)


def _split(escaped: str, room: int) -> list[str]:
    """Split on spaces into chunks of <= room UTF-8 bytes (escaping never creates spaces)."""
    if room < 1:
        raise ValueError("The buffer Sonic announces leaves no room for the text")
    chunks: list[str] = []
    cur = ""
    for word in escaped.split(" "):
        size = len(word.encode())
        if size > room:
            raise ValueError(
                f"A {size}-byte word does not fit in the Sonic buffer ({room} usable): "
                "shorten or remove it before indexing."
            )
        joined = f"{cur} {word}" if cur else word
        if len(joined.encode()) <= room:
            cur = joined
        else:
            chunks.append(cur)
            cur = word
    chunks.append(cur)
    return chunks


class Sonic:
    """Sonic client. Everything is a flat method; the mode (search/ingest/control), the
    handshake and the connection pool are handled by the class.

    >>> async with Sonic("localhost", 1491, "SecretPassword") as sonic:  # doctest: +SKIP
    ...     await sonic.push("videos", "catalog", "video:1", "cats and dogs", lang="eng")

    Connections: opened on first use, up to `pool_size` per channel, with pipelining (each
    connection allows `max_in_flight` simultaneous commands; `None` = unlimited). No retries:
    if a connection drops, its in-flight commands fail with `SonicConnectionError` and the
    next command opens a new connection. `timeout` is per command; when it expires
    `SonicTimeout` is raised but the connection stays alive.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 1491,
        password: str = "",
        *,
        pool_size: int = 4,
        max_in_flight: int | None = None,
        timeout: float = 10.0,
        connect_timeout: float = 5.0,
    ) -> None:
        self.host, self.port = host, port
        self.password = _token(password, "password") if password else ""
        self.pool_size, self.max_in_flight = max(1, pool_size), max_in_flight
        self.timeout, self.connect_timeout = timeout, connect_timeout
        self._search = _Pool("search", self)
        self._ingest = _Pool("ingest", self)
        self._control = _Pool("control", self)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        """Close every connection (QUIT). E.g. ``await sonic.close()``. Never raises."""
        await asyncio.gather(self._search.close(), self._ingest.close(), self._control.close())

    # -- search ---------------------------------------------------------------------

    async def query(
        self,
        collection: str,
        bucket: str,
        terms: str,
        *,
        limit: int | None = None,
        offset: int | None = None,
        lang: str | None = None,
    ) -> list[str]:
        """Object ids matching `terms`, best first. E.g. ``await sonic.query("videos", "catalog", "cats", limit=10)``."""
        line = (
            f"QUERY {_token(collection, 'collection')} {_token(bucket, 'bucket')} {quote(terms)}"
            + _opts(limit=limit, offset=offset, lang=lang)
        )
        return (await self._search.call(line, "QUERY")).split()

    async def suggest(
        self, collection: str, bucket: str, word: str, *, limit: int | None = None
    ) -> list[str]:
        """Words that complete `word`. E.g. ``await sonic.suggest("videos", "catalog", "ca")``."""
        line = (
            f"SUGGEST {_token(collection, 'collection')} {_token(bucket, 'bucket')} {quote(word)}"
            + _opts(limit=limit)
        )
        return (await self._search.call(line, "SUGGEST")).split()

    async def list_words(
        self, collection: str, bucket: str, *, limit: int | None = None, offset: int | None = None
    ) -> list[str]:
        """Indexed words of the bucket (LIST enumerates words, not objects). E.g. ``await sonic.list_words("videos", "catalog", limit=50)``."""
        line = f"LIST {_token(collection, 'collection')} {_token(bucket, 'bucket')}" + _opts(
            limit=limit, offset=offset
        )
        return (await self._search.call(line, "LIST")).split()

    # -- ingest ----------------------------------------------------------------------

    async def push(
        self, collection: str, bucket: str, object: str, text: str, *, lang: str | None = None
    ) -> None:
        """Index `text` for `object`. If it does not fit in the Sonic buffer it is split on words. E.g. ``await sonic.push("videos", "catalog", "video:1", "cats and dogs", lang="eng")``."""
        head = f"PUSH {_token(collection, 'collection')} {_token(bucket, 'bucket')} "
        head += f"{_token(object, 'object')} "
        tail = _opts(lang=lang)
        conn = await self._ingest.get()
        room = conn.buffer - len((head + tail).encode()) - 3  # 2 quotes + newline
        for chunk in _split(quote(text)[1:-1], room):
            _ok(await conn.call(f'{head}"{chunk}"{tail}'))

    async def pop(self, collection: str, bucket: str, object: str, text: str) -> int:
        """Remove the words of `text` from the object; returns how many. E.g. ``await sonic.pop("videos", "catalog", "video:1", "cats")``."""
        line = (
            f"POP {_token(collection, 'collection')} {_token(bucket, 'bucket')} "
            f"{_token(object, 'object')} {quote(text)}"
        )
        return _int(await self._ingest.call(line))

    async def count(
        self, collection: str, bucket: str | None = None, object: str | None = None
    ) -> int:
        """Buckets of the collection, objects of the bucket or terms of the object. E.g. ``await sonic.count("videos", "catalog")``.

        Uses `COUNT`, not `COUNTC/B/O`: PROTOCOL.md lists them but Sonic v1.9.1 answers
        `ERR unknown_command`. Note: on v1.9.1 `count(collection, bucket)` returns distinct
        words, not objects.
        """
        if bucket is None and object is not None:
            raise ValueError("count: `object` requires `bucket`.")
        parts = [_token(collection, "collection")]
        if bucket is not None:
            parts.append(_token(bucket, "bucket"))
        if object is not None:
            parts.append(_token(object, "object"))
        return _int(await self._ingest.call("COUNT " + " ".join(parts)))

    async def flush_collection(self, collection: str) -> int:
        """Delete the whole collection; returns how many items. E.g. ``await sonic.flush_collection("videos")``."""
        return _int(await self._ingest.call(f"FLUSHC {_token(collection, 'collection')}"))

    async def flush_bucket(self, collection: str, bucket: str) -> int:
        """Delete a bucket. E.g. ``await sonic.flush_bucket("videos", "catalog")``."""
        line = f"FLUSHB {_token(collection, 'collection')} {_token(bucket, 'bucket')}"
        return _int(await self._ingest.call(line))

    async def flush_object(self, collection: str, bucket: str, object: str) -> int:
        """Delete an object (does not clean SUGGEST). E.g. ``await sonic.flush_object("videos", "catalog", "video:1")``."""
        line = (
            f"FLUSHO {_token(collection, 'collection')} {_token(bucket, 'bucket')} "
            f"{_token(object, 'object')}"
        )
        return _int(await self._ingest.call(line))

    # -- control ----------------------------------------------------------------------

    async def trigger(self, action: str | None = None, data: str | None = None) -> str:
        """`TRIGGER [action] [data]`; actions: consolidate, backup, restore. Returns the result ("" if Sonic answers OK; with no action, the list of actions). E.g. ``await sonic.trigger("consolidate")``."""
        if action is None and data is not None:
            raise ValueError("trigger: `data` requires `action`.")
        line = "TRIGGER"
        if action is not None:
            line += " " + _token(action, "action")
        if data is not None:
            line += " " + _token(data, "data")
        reply = await self._control.call(line)
        return "" if reply == "OK" else _result(reply)

    async def info(self) -> dict[str, int]:
        """Server metrics (`uptime`, `clients_connected`...). E.g. ``(await sonic.info())["uptime"]``."""
        return {k: int(v) for k, v in _KV.findall(_result(await self._control.call("INFO")))}

    async def ping(self) -> None:
        """Check that Sonic answers (raises otherwise). E.g. ``await sonic.ping()``."""
        reply = await self._control.call("PING")
        if reply != "PONG":
            raise SonicProtocolError(f"Expected PONG, got {reply!r}")

    async def help(self, manual: str | None = None) -> str:
        """`HELP [manual]` as is. E.g. ``await sonic.help("commands")``."""
        line = "HELP" if manual is None else f"HELP {_token(manual, 'manual')}"
        return _result(await self._control.call(line))
