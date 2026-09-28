"""Fake Sonic server: speaks the real protocol (CONNECTED / START / STARTED buffer(N)).

Every command line after START goes to `handler(cmd, fake, writer)`; the handler answers with
`fake.say(writer, "...")` (appending \\r\\n like Sonic) or by closing `writer`.
"""

import asyncio
from collections.abc import Awaitable, Callable

Handler = Callable[[str, "FakeSonic", asyncio.StreamWriter], Awaitable[None]]


class FakeSonic:
    def __init__(self, handler: Handler, *, password: str = "pw", buffer: int = 20000) -> None:
        self.handler, self.password, self.buffer = handler, password, buffer
        self.received: list[str] = []
        self.modes: list[str] = []  # one item per accepted connection
        self.tasks: set[asyncio.Task[None]] = set()
        self.server: asyncio.Server | None = None
        self.port = 0
        self.greeting = "CONNECTED <sonic-server v1.9.1>"
        self.started_suffix: str | None = None  # to force an odd STARTED

    async def __aenter__(self) -> FakeSonic:
        self.server = await asyncio.start_server(self._serve, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc: object) -> None:
        for t in self.tasks:
            t.cancel()
        assert self.server is not None
        self.server.close()

    @staticmethod
    async def say(writer: asyncio.StreamWriter, text: str) -> None:
        writer.write(text.encode() + b"\r\n")
        await writer.drain()

    def later(self, delay: float, writer: asyncio.StreamWriter, text: str) -> None:
        async def go() -> None:
            await asyncio.sleep(delay)
            try:
                await self.say(writer, text)
            except OSError:
                pass

        task = asyncio.create_task(go())
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await self.say(writer, self.greeting)
        start = (await reader.readline()).decode().split()
        if self.password != start[2] if len(start) > 2 else self.password != "":
            await self.say(writer, "ENDED authentication_failed")  # like the real Sonic
            writer.close()
            return
        self.modes.append(start[1])
        tail = self.started_suffix or f"protocol(1) buffer({self.buffer})"
        await self.say(writer, f"STARTED {start[1]} {tail}")
        while raw := await reader.readline():
            cmd = raw.decode().rstrip("\n")
            self.received.append(cmd)
            if cmd == "QUIT":
                await self.say(writer, "ENDED quit")
                writer.close()
                return
            await self.handler(cmd, self, writer)
