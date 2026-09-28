"""Cliente asyncio de Sonic (https://github.com/valeriansaliou/sonic), sin dependencias.

Uso: ``async with Sonic("localhost", 1491, "password") as sonic: await sonic.query(...)``.
Detalle completo en README.md y llms.txt.
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
    """Base de todos los errores de esta libreria."""


class SonicConnectionError(SonicError):
    """No se pudo conectar, o la conexion se cayo o esta cerrada.

    Los comandos en vuelo en esa conexion fallan con esto; el siguiente comando abre otra.
    """


class SonicTimeout(SonicConnectionError):
    """Vencio `connect_timeout` (al conectar) o `timeout` (por comando)."""


class SonicServerError(SonicError):
    """Sonic contesto `ERR <codigo>(<detalle>)`. `.code` es el codigo, `.line` la linea entera."""

    def __init__(self, line: str) -> None:
        match = re.match(r"ERR (\w+)", line)
        self.line = line
        self.code = match.group(1) if match else ""
        hint = _HINTS.get(self.code, "")
        super().__init__(f"Sonic rechazo el comando: {line}" + (f". {hint}" if hint else ""))


class SonicProtocolError(SonicError):
    """Sonic dijo algo que PROTOCOL.md no contempla (version incompatible?). La conexion se cierra."""


_HINTS = {
    "authentication_failed": "Password incorrecto: usa `channel.auth_password` de sonic.cfg.",
    "invalid_format": "Formato de comando invalido: si solo usas la API publica, es un bug de "
    "async-sonic; abre un issue con el texto que enviaste.",
    "policy_reject": "Valor fuera de los limites del servidor (limit/offset/texto): revisa "
    "`[search]` y `[store]` en sonic.cfg.",
    "unknown_command": "Esa version de Sonic no conoce el comando.",
    "not_found": "Sonic no conoce ese recurso o accion (p. ej. trigger: consolidate, backup, restore).",
}
_BUFFER = re.compile(r"buffer\((\d+)\)")
_KV = re.compile(r"(\w+)\((-?\d+)\)")


def quote(text: str) -> str:
    """Texto entre comillas para PUSH/POP/QUERY/SUGGEST. Ej.: ``quote('di "hola"')``.

    PROTOCOL.md solo exige `\\"` para las comillas; la barra invertida se duplica para que
    una barra final no se coma la comilla de cierre. Los saltos de linea pasan a espacio:
    un salto crudo terminaria el comando a medias (Sonic tokeniza por espacios, no se pierde
    ninguna palabra).
    """
    flat = text.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    return '"' + flat.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _token(value: str, what: str) -> str:
    if not value or any(c.isspace() or c == '"' or not c.isprintable() for c in value):
        raise ValueError(
            f"{what} invalido {value!r}: no puede estar vacio ni llevar espacios, comillas "
            "o caracteres de control. Usa un nombre corto como 'videos' o 'video:42'."
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
            raise ValueError(f"lang invalido {lang!r}: usa un codigo ISO 639-3 ('spa') o 'none'.")
        out += f" LANG({lang})"
    return out


def _settle(fut: asyncio.Future[str], value: str | Exception) -> None:
    if not fut.done():
        if isinstance(value, Exception):
            fut.set_exception(value)
        else:
            fut.set_result(value)


class _Conn:
    """Una conexion TCP en un modo, con un lector en segundo plano.

    Pipelining: cada comando se escribe sin esperar. Las respuestas inmediatas (OK, RESULT,
    PONG, PENDING <id>, ERR) llegan en orden -> cola FIFO de futuros. Los `EVENT` de
    QUERY/SUGGEST/LIST (PROTOCOL.md: pueden llegar desordenados) se casan por id.
    Un comando que vence su timeout deja su hueco en la cola/tabla: la respuesta tardia se
    descarta al llegar, asi la conexion nunca se desincroniza.
    """

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader, self._writer = reader, writer
        self.buffer = 0
        self.load = 0  # llamadas activas, para elegir conexion
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
                            f"{where} no habla el protocolo de Sonic (saludo {greeting!r}): "
                            "comprueba que el puerto es el de Sonic Channel (1491 por defecto)."
                        )
                    writer.write(f"START {mode} {password}\n".encode())
                    started = await conn._readline()
                    match = _BUFFER.search(started)
                    if not started.startswith(f"STARTED {mode} ") or match is None:
                        raise SonicProtocolError(f"respuesta a START inesperada: {started!r}")
                    conn.buffer = int(match.group(1))
                except BaseException:
                    writer.close()
                    raise
        except TimeoutError:
            raise SonicTimeout(
                f"Timeout ({connect_timeout}s) al conectar con Sonic en {where}: comprueba "
                "host/puerto o sube `connect_timeout`."
            ) from None
        except OSError as exc:
            raise SonicConnectionError(
                f"No se pudo conectar con Sonic en {where} ({exc}): comprueba que Sonic esta "
                "en marcha y que host y puerto son correctos."
            ) from exc
        conn.timeout = timeout
        conn.gate = asyncio.Semaphore(max_in_flight) if max_in_flight else None
        conn._task = asyncio.create_task(conn._run())
        return conn

    async def _readline(self) -> str:
        try:
            raw = await self._reader.readline()
        except ValueError as exc:  # mas larga que `limit`
            raise SonicProtocolError("Sonic envio una linea de mas de 1 MiB") from exc
        except OSError as exc:
            raise SonicConnectionError(f"Se perdio la conexion con Sonic ({exc}).") from exc
        if not raw.endswith(b"\n"):
            raise SonicConnectionError(
                "Sonic cerro la conexion (reinicio, `tcp_timeout` o caida): el siguiente "
                "comando abrira una conexion nueva."
            )
        line = raw.decode(errors="replace").rstrip("\r\n")
        if line.startswith("ENDED authentication_failed") and self._task is None:
            raise SonicServerError("ERR authentication_failed")  # asi contesta Sonic a START
        if line.startswith("ENDED "):
            raise SonicConnectionError(f"Sonic termino la sesion ({line}).")
        if line.startswith("ERR ") and self._task is None:  # durante el handshake
            raise SonicServerError(line)
        return line

    async def _run(self) -> None:
        exc: Exception
        try:
            while True:
                self._dispatch(await self._readline())
        except SonicError as e:
            exc = e
        except Exception as e:  # ponytail: lo inesperado tambien cierra la conexion
            exc = SonicProtocolError(f"error interno leyendo de Sonic: {e!r}")
        self._shutdown(exc)

    def _dispatch(self, line: str) -> None:
        if line.startswith("EVENT "):
            parts = line.split(" ", 3)
            entry = self._events.pop(parts[2], None) if len(parts) > 2 else None
            if entry is None:
                raise SonicProtocolError(f"EVENT sin PENDING previo: {line!r}")
            fut, name = entry
            if parts[1] != name:
                _settle(fut, SonicProtocolError(f"se esperaba EVENT {name}, llego {line!r}"))
            else:
                _settle(fut, parts[3] if len(parts) > 3 else "")
            return
        if not self._fifo:
            raise SonicProtocolError(f"respuesta sin comando pendiente: {line!r}")
        fut, event = self._fifo.popleft()
        if line.startswith("ERR "):
            _settle(fut, SonicServerError(line))
        elif event is None and not line.startswith("PENDING "):
            _settle(fut, line)
        elif event is not None and line.startswith("PENDING "):
            self._events[line.removeprefix("PENDING ").strip()] = (fut, event)
        else:
            _settle(fut, SonicProtocolError(f"respuesta inesperada: {line!r}"))

    def _shutdown(self, exc: Exception) -> None:
        self.closed = True
        self._writer.close()
        pending = [f for f, _ in self._fifo] + [f for f, _ in self._events.values()]
        self._fifo.clear()
        self._events.clear()
        for fut in pending:
            _settle(fut, exc)

    async def call(self, line: str, event: str | None = None) -> str:
        """Un comando. Con `event`, devuelve lo que sigue a `EVENT <event> <id>`; sin el, la
        primera linea de respuesta."""
        if self.closed:
            raise SonicConnectionError(
                "Conexion cerrada: abre un `async with Sonic(...)` nuevo, o vuelve a llamar "
                "(el pool abre una conexion nueva)."
            )
        data = line.encode() + b"\n"
        if len(data) > self.buffer:
            raise ValueError(
                f"Comando de {len(data)} bytes; el buffer que anuncia Sonic es {self.buffer}. "
                "Acorta el texto (PUSH lo trocea solo; QUERY/SUGGEST/POP no)."
            )
        self.load += 1
        try:
            if self.gate is not None:
                await self.gate.acquire()
            try:
                fut: asyncio.Future[str] = asyncio.get_running_loop().create_future()
                self._fifo.append((fut, event))  # cola y escritura sin await en medio: mismo orden
                self._writer.write(data)
                try:
                    async with asyncio.timeout(self.timeout):
                        await self._writer.drain()
                        return await fut
                except TimeoutError:
                    raise SonicTimeout(
                        f"Sonic no contesto en {self.timeout}s a {line[:40]!r}: sube `timeout=` "
                        "o revisa la carga del servidor. La conexion sigue usable."
                    ) from None
                except OSError as exc:
                    raise SonicConnectionError(f"Se perdio la conexion con Sonic ({exc}).") from exc
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
                await self._task  # el servidor contesta ENDED y cierra
        except TimeoutError, OSError:
            pass
        finally:
            self._task.cancel()
            self._shutdown(SonicConnectionError("Conexion cerrada por el cliente."))


class _Pool:
    """Hasta `size` conexiones de un modo, abiertas cuando hacen falta. Elige la menos cargada."""

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
        raise SonicProtocolError(f"se esperaba OK, llego {reply!r}")


def _result(reply: str) -> str:
    if not reply.startswith("RESULT "):
        raise SonicProtocolError(f"se esperaba RESULT, llego {reply!r}")
    return reply.removeprefix("RESULT ")


def _int(reply: str) -> int:
    text = _result(reply)
    if not text.isdecimal():
        raise SonicProtocolError(f"se esperaba un entero, llego {reply!r}")
    return int(text)


def _split(escaped: str, room: int) -> list[str]:
    """Trocea por espacios en pedazos de <= room bytes UTF-8 (el escapado no crea espacios)."""
    if room < 1:
        raise ValueError("el buffer que anuncia Sonic no deja sitio para el texto")
    chunks: list[str] = []
    cur = ""
    for word in escaped.split(" "):
        size = len(word.encode())
        if size > room:
            raise ValueError(
                f"Una palabra de {size} bytes no cabe en el buffer de Sonic ({room} utiles): "
                "acortala o quitala antes de indexar."
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
    """Cliente de Sonic. Todo son metodos planos; el modo (search/ingest/control), el
    handshake y el pool de conexiones son cosa de la clase.

    >>> async with Sonic("localhost", 1491, "SecretPassword") as sonic:  # doctest: +SKIP
    ...     await sonic.push("videos", "catalogo", "video:1", "gatos y perros", lang="spa")

    Conexiones: abiertas al primer uso, hasta `pool_size` por canal, con pipelining (cada
    conexion admite `max_in_flight` comandos a la vez; `None` = sin tope). Sin reintentos: si
    una conexion se cae, sus comandos en vuelo fallan con `SonicConnectionError` y el
    siguiente comando abre una conexion nueva. `timeout` es por comando; vencerlo lanza
    `SonicTimeout` pero la conexion sigue viva.
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
        """Cierra todas las conexiones (QUIT). Ej.: ``await sonic.close()``. Nunca lanza."""
        await asyncio.gather(self._search.close(), self._ingest.close(), self._control.close())

    # -- busqueda ---------------------------------------------------------------------

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
        """Ids de objeto que casan con `terms`, mejor primero. Ej.: ``await sonic.query("videos", "catalogo", "gatos", limit=10)``."""
        line = (
            f"QUERY {_token(collection, 'collection')} {_token(bucket, 'bucket')} {quote(terms)}"
            + _opts(limit=limit, offset=offset, lang=lang)
        )
        return (await self._search.call(line, "QUERY")).split()

    async def suggest(
        self, collection: str, bucket: str, word: str, *, limit: int | None = None
    ) -> list[str]:
        """Palabras que completan `word`. Ej.: ``await sonic.suggest("videos", "catalogo", "gat")``."""
        line = (
            f"SUGGEST {_token(collection, 'collection')} {_token(bucket, 'bucket')} {quote(word)}"
            + _opts(limit=limit)
        )
        return (await self._search.call(line, "SUGGEST")).split()

    async def list_words(
        self, collection: str, bucket: str, *, limit: int | None = None, offset: int | None = None
    ) -> list[str]:
        """Palabras indexadas del bucket (LIST enumera palabras, no objetos). Ej.: ``await sonic.list_words("videos", "catalogo", limit=50)``."""
        line = f"LIST {_token(collection, 'collection')} {_token(bucket, 'bucket')}" + _opts(
            limit=limit, offset=offset
        )
        return (await self._search.call(line, "LIST")).split()

    # -- ingesta ----------------------------------------------------------------------

    async def push(
        self, collection: str, bucket: str, object: str, text: str, *, lang: str | None = None
    ) -> None:
        """Indexa `text` para `object`. Si no cabe en el buffer de Sonic, se trocea por palabras. Ej.: ``await sonic.push("videos", "catalogo", "video:1", "gatos y perros", lang="spa")``."""
        head = f"PUSH {_token(collection, 'collection')} {_token(bucket, 'bucket')} "
        head += f"{_token(object, 'object')} "
        tail = _opts(lang=lang)
        conn = await self._ingest.get()
        room = conn.buffer - len((head + tail).encode()) - 3  # 2 comillas + salto de linea
        for chunk in _split(quote(text)[1:-1], room):
            _ok(await conn.call(f'{head}"{chunk}"{tail}'))

    async def pop(self, collection: str, bucket: str, object: str, text: str) -> int:
        """Quita las palabras de `text` del objeto; devuelve cuantas. Ej.: ``await sonic.pop("videos", "catalogo", "video:1", "gatos")``."""
        line = (
            f"POP {_token(collection, 'collection')} {_token(bucket, 'bucket')} "
            f"{_token(object, 'object')} {quote(text)}"
        )
        return _int(await self._ingest.call(line))

    async def count(
        self, collection: str, bucket: str | None = None, object: str | None = None
    ) -> int:
        """Buckets de la coleccion, objetos del bucket o terminos del objeto. Ej.: ``await sonic.count("videos", "catalogo")``.

        Usa `COUNT`, no `COUNTC/B/O`: PROTOCOL.md los lista pero Sonic v1.9.1 contesta
        `ERR unknown_command`. Ojo: en v1.9.1 `count(col, bucket)` devuelve palabras distintas,
        no objetos.
        """
        if bucket is None and object is not None:
            raise ValueError("count: `object` requiere `bucket`.")
        parts = [_token(collection, "collection")]
        if bucket is not None:
            parts.append(_token(bucket, "bucket"))
        if object is not None:
            parts.append(_token(object, "object"))
        return _int(await self._ingest.call("COUNT " + " ".join(parts)))

    async def flush_collection(self, collection: str) -> int:
        """Borra toda la coleccion; devuelve cuantos elementos. Ej.: ``await sonic.flush_collection("videos")``."""
        return _int(await self._ingest.call(f"FLUSHC {_token(collection, 'collection')}"))

    async def flush_bucket(self, collection: str, bucket: str) -> int:
        """Borra un bucket. Ej.: ``await sonic.flush_bucket("videos", "catalogo")``."""
        line = f"FLUSHB {_token(collection, 'collection')} {_token(bucket, 'bucket')}"
        return _int(await self._ingest.call(line))

    async def flush_object(self, collection: str, bucket: str, object: str) -> int:
        """Borra un objeto (no limpia SUGGEST). Ej.: ``await sonic.flush_object("videos", "catalogo", "video:1")``."""
        line = (
            f"FLUSHO {_token(collection, 'collection')} {_token(bucket, 'bucket')} "
            f"{_token(object, 'object')}"
        )
        return _int(await self._ingest.call(line))

    # -- control ----------------------------------------------------------------------

    async def trigger(self, action: str | None = None, data: str | None = None) -> str:
        """`TRIGGER [action] [data]`; acciones: consolidate, backup, restore. Devuelve el resultado ("" si Sonic contesta OK; sin accion, la lista de acciones). Ej.: ``await sonic.trigger("consolidate")``."""
        if action is None and data is not None:
            raise ValueError("trigger: `data` requiere `action`.")
        line = "TRIGGER"
        if action is not None:
            line += " " + _token(action, "action")
        if data is not None:
            line += " " + _token(data, "data")
        reply = await self._control.call(line)
        return "" if reply == "OK" else _result(reply)

    async def info(self) -> dict[str, int]:
        """Metricas del servidor (`uptime`, `clients_connected`...). Ej.: ``(await sonic.info())["uptime"]``."""
        return {k: int(v) for k, v in _KV.findall(_result(await self._control.call("INFO")))}

    async def ping(self) -> None:
        """Comprueba que Sonic responde (lanza si no). Ej.: ``await sonic.ping()``."""
        reply = await self._control.call("PING")
        if reply != "PONG":
            raise SonicProtocolError(f"se esperaba PONG, llego {reply!r}")

    async def help(self, manual: str | None = None) -> str:
        """`HELP [manual]` tal cual. Ej.: ``await sonic.help("commands")``."""
        line = "HELP" if manual is None else f"HELP {_token(manual, 'manual')}"
        return _result(await self._control.call(line))
