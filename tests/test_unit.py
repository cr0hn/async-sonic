import asyncio
import time
from collections.abc import Awaitable, Callable

import pytest

from async_sonic import (
    Sonic,
    SonicConnectionError,
    SonicProtocolError,
    SonicServerError,
    SonicTimeout,
    quote,
)
from tests.fake_sonic import FakeSonic, Handler


async def echo_ok(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
    """Minimal handler: PING -> PONG, everything else -> OK."""
    await fake.say(w, "PONG" if cmd == "PING" else "OK")


def client(fake: FakeSonic, **kw: int | float | None) -> Sonic:
    return Sonic("127.0.0.1", fake.port, "pw", **kw)  # pyright: ignore[reportArgumentType]


def control_conn(s: Sonic):
    return s._control.conns[0]  # pyright: ignore[reportPrivateUsage]


# ---- escaping: expected values written by hand, not derived from the function -----------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("hello", '"hello"'),
        ('say "hi"', '"say \\"hi\\""'),
        ("a\\b", '"a\\\\b"'),
        ("ends with backslash\\", '"ends with backslash\\\\"'),
        ('\\"', '"\\\\\\""'),
        ("line1\nline2", '"line1 line2"'),
        ("a\r\nb\rc", '"a b c"'),
        ("canción ñandú 日本 😀", '"canción ñandú 日本 😀"'),
        ("", '""'),
    ],
)
def test_quote(text: str, expected: str) -> None:
    assert quote(text) == expected


# ---- handshake ----------------------------------------------------------------------------


async def test_handshake_sends_start_with_mode_and_reads_buffer() -> None:
    async with FakeSonic(echo_ok, buffer=1234) as fake, client(fake) as s:
        await s.ping()
        assert fake.modes == ["control"]  # ping goes through the control channel
        assert control_conn(s).buffer == 1234


async def test_wrong_password() -> None:
    async with FakeSonic(echo_ok, password="other") as fake, client(fake) as s:
        with pytest.raises(SonicServerError) as ei:
            await s.ping()
        assert ei.value.code == "authentication_failed"
        assert "channel.auth_password" in str(ei.value)


async def test_greeting_is_not_sonic() -> None:
    async with FakeSonic(echo_ok) as fake:
        fake.greeting = "HTTP/1.1 400 Bad Request"
        async with client(fake) as s:
            with pytest.raises(SonicProtocolError, match="does not speak the Sonic protocol"):
                await s.ping()


async def test_started_without_buffer() -> None:
    async with FakeSonic(echo_ok) as fake:
        fake.started_suffix = "protocol(1)"
        async with client(fake) as s:
            with pytest.raises(SonicProtocolError, match="Unexpected reply to START"):
                await s.ping()


async def test_connection_refused() -> None:
    async with FakeSonic(echo_ok) as fake:
        port = fake.port
    async with Sonic("127.0.0.1", port, "pw") as s:
        with pytest.raises(SonicConnectionError, match="Could not connect") as ei:
            await s.ping()
        assert not isinstance(ei.value, SonicTimeout)


async def test_connect_timeout() -> None:
    async def mute(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await asyncio.sleep(5)  # accepts and never greets

    server = await asyncio.start_server(mute, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        async with Sonic("127.0.0.1", port, "pw", connect_timeout=0.1) as s:
            with pytest.raises(SonicTimeout, match=r"connecting to Sonic"):
                await s.ping()
    finally:
        server.close()


# ---- commands: exact line sent + reply parsing --------------------------------------------


async def test_search_commands() -> None:
    async def h(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
        verb = cmd.split()[0]
        results = {"QUERY": "video:1 video:2", "SUGGEST": "cat cats", "LIST": ""}
        await fake.say(w, "PENDING m1")
        await fake.say(w, f"EVENT {verb} m1 {results[verb]}")

    async with FakeSonic(h) as fake, client(fake) as s:
        assert await s.query("c", "b", 'say "x"', limit=5, offset=2, lang="eng") == [
            "video:1",
            "video:2",
        ]
        assert await s.query("c", "b", "y") == ["video:1", "video:2"]
        assert await s.suggest("c", "b", "ca", limit=3) == ["cat", "cats"]
        assert await s.list_words("c", "b", limit=9, offset=1) == []
        assert fake.received == [
            'QUERY c b "say \\"x\\"" LIMIT(5) OFFSET(2) LANG(eng)',
            'QUERY c b "y"',
            'SUGGEST c b "ca" LIMIT(3)',
            "LIST c b LIMIT(9) OFFSET(1)",
        ]
        assert fake.modes == ["search"]


async def test_ingest_commands() -> None:
    async def h(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
        await fake.say(w, "OK" if cmd.startswith("PUSH") else "RESULT 7")

    async with FakeSonic(h) as fake, client(fake) as s:
        await s.push("c", "b", "o:1", "hello", lang="eng")
        assert await s.pop("c", "b", "o:1", "hello") == 7
        assert await s.count("c") == 7
        assert await s.count("c", "b") == 7
        assert await s.count("c", "b", "o:1") == 7
        assert await s.flush_collection("c") == 7
        assert await s.flush_bucket("c", "b") == 7
        assert await s.flush_object("c", "b", "o:1") == 7
        assert fake.received == [
            'PUSH c b o:1 "hello" LANG(eng)',
            'POP c b o:1 "hello"',
            "COUNT c",
            "COUNT c b",
            "COUNT c b o:1",
            "FLUSHC c",
            "FLUSHB c b",
            "FLUSHO c b o:1",
        ]
        assert fake.modes == ["ingest"]


async def test_control_commands() -> None:
    async def h(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
        replies = {
            "TRIGGER consolidate": "OK",
            "TRIGGER": "RESULT actions(consolidate, backup, restore)",
            "INFO": "RESULT uptime(11) clients_connected(2) command_latency_best(6)",
            "HELP commands": "RESULT commands(TRIGGER, INFO, PING, HELP, QUIT)",
            "PING": "PONG",
        }
        await fake.say(w, replies[cmd])

    async with FakeSonic(h) as fake, client(fake) as s:
        assert await s.trigger("consolidate") == ""
        assert await s.trigger() == "actions(consolidate, backup, restore)"
        assert await s.info() == {"uptime": 11, "clients_connected": 2, "command_latency_best": 6}
        assert await s.help("commands") == "commands(TRIGGER, INFO, PING, HELP, QUIT)"
        await s.ping()


CALLS_INVALIDAS: list[Callable[[Sonic], Awaitable[object]]] = [
    lambda s: s.query("with space", "b", "x"),
    lambda s: s.push("c", "b", 'o"1', "x"),
    lambda s: s.query("c", "b", "x", lang="es;"),
    lambda s: s.count("c", None, "obj"),
    lambda s: s.trigger(None, "data"),
]


@pytest.mark.parametrize("call", CALLS_INVALIDAS)
async def test_invalid_arguments_never_reach_the_server(
    call: Callable[[Sonic], Awaitable[object]],
) -> None:
    async with FakeSonic(echo_ok) as fake, client(fake) as s:
        with pytest.raises(ValueError):
            await call(s)
        assert fake.received == []


# ---- PENDING / EVENT ----------------------------------------------------------------------


async def test_out_of_order_events_are_matched_by_id() -> None:
    ids = iter(["idA", "idB"])
    pending: dict[str, str] = {}

    async def h(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
        mid = next(ids)
        pending[cmd.split('"')[1]] = mid
        await fake.say(w, f"PENDING {mid}")
        if len(pending) == 2:  # answers in reverse order of arrival
            await fake.say(w, f"EVENT QUERY {pending['second']} res-second")
            await fake.say(w, f"EVENT QUERY {pending['first']} res-first")

    async with FakeSonic(h) as fake, client(fake, pool_size=1) as s:
        a = asyncio.create_task(s.query("c", "b", "first"))
        await asyncio.sleep(0.05)
        b = asyncio.create_task(s.query("c", "b", "second"))
        assert await a == ["res-first"]
        assert await b == ["res-second"]
        assert len(fake.modes) == 1


async def test_event_of_another_type() -> None:
    async def h(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
        await fake.say(w, "PENDING m")
        await fake.say(w, "EVENT SUGGEST m algo")

    async with FakeSonic(h) as fake, client(fake) as s:
        with pytest.raises(SonicProtocolError, match="Expected EVENT QUERY"):
            await s.query("c", "b", "x")


async def test_event_without_pending_closes_the_connection() -> None:
    async def h(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
        await fake.say(w, "PENDING m")
        await fake.say(w, "EVENT QUERY other-id x")

    async with FakeSonic(h) as fake, client(fake) as s:
        with pytest.raises(SonicProtocolError, match="EVENT without a previous PENDING"):
            await s.query("c", "b", "x")


async def test_unsolicited_reply_closes_the_connection() -> None:
    async def h(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
        await fake.say(w, "PONG")
        await fake.say(w, "PONG")  # extra one

    async with FakeSonic(h) as fake, client(fake) as s:
        await s.ping()
        await asyncio.sleep(0.05)
        assert control_conn(s).closed


async def test_err_does_not_break_the_connection() -> None:
    async def h(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
        if cmd.startswith("QUERY"):
            await fake.say(w, "ERR policy_reject(LIMIT out of minimum/maximum bounds)")
        else:
            await fake.say(w, "PONG")

    async with FakeSonic(h) as fake, client(fake) as s:
        with pytest.raises(SonicServerError, match="policy_reject") as ei:
            await s.query("c", "b", "x", limit=0)
        assert ei.value.code == "policy_reject"
        with pytest.raises(SonicServerError):  # the same connection is still in sync
            await s.query("c", "b", "y", limit=0)
        assert len(fake.modes) == 1


# ---- drops and timeouts --------------------------------------------------------------------


async def test_abrupt_close_and_recovery() -> None:
    calls = 0

    async def h(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            w.close()  # hangs up without answering
        else:
            await fake.say(w, "PONG")

    async with FakeSonic(h) as fake, client(fake) as s:
        with pytest.raises(SonicConnectionError, match="Sonic closed the connection"):
            await s.ping()
        await s.ping()  # no magic retry: this is ANOTHER command, and it opens a new connection
        assert len(fake.modes) == 2


async def test_server_ended() -> None:
    async def h(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
        await fake.say(w, "ENDED timeout")

    async with FakeSonic(h) as fake, client(fake) as s:
        with pytest.raises(SonicConnectionError, match="ENDED timeout"):
            await s.ping()


async def test_command_timeout_and_late_reply_do_not_desync() -> None:
    async def h(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
        if cmd.startswith("QUERY"):
            await fake.say(w, "PENDING slow")
            fake.later(0.3, w, "EVENT QUERY slow late")
        else:
            await fake.say(w, "PONG")

    async with FakeSonic(h) as fake, client(fake, timeout=0.1) as s:
        with pytest.raises(SonicTimeout, match=r"within 0.1s"):
            await s.query("c", "b", "x")
        await asyncio.sleep(0.4)  # the late EVENT arrives and is discarded
        await s.ping()
        assert fake.modes == ["search", "control"]
        assert not s._search.conns[0].closed  # pyright: ignore[reportPrivateUsage]


async def test_close_sends_quit_and_blocks_the_connection() -> None:
    async with FakeSonic(echo_ok) as fake:
        s = client(fake)
        await s.ping()
        conn = control_conn(s)
        await s.close()
        assert fake.received[-1] == "QUIT"
        assert conn.closed
        with pytest.raises(SonicConnectionError, match="Connection closed"):
            await conn.call("PING")


# ---- buffer -------------------------------------------------------------------------------


async def test_push_is_split_to_fit_the_buffer() -> None:
    text = " ".join(f'word{i}"' for i in range(40)) + " canción\\"
    async with FakeSonic(echo_ok, buffer=80) as fake, client(fake) as s:
        await s.push("c", "b", "o", text, lang="eng")
        assert len(fake.received) > 3
        words: list[str] = []
        for line in fake.received:
            assert len(line.encode()) + 1 <= 80
            assert line.startswith('PUSH c b o "') and line.endswith('" LANG(eng)')
            words += line[len('PUSH c b o "') : -len('" LANG(eng)')].split(" ")
        # words arrive whole and in order (quotes and backslash escaped)
        expected_words = [f'word{i}\\"' for i in range(40)] + ["canción\\\\"]
        assert words == expected_words


async def test_word_larger_than_the_buffer() -> None:
    async with FakeSonic(echo_ok, buffer=40) as fake, client(fake) as s:
        with pytest.raises(ValueError, match="does not fit in the Sonic buffer"):
            await s.push("c", "b", "o", "x" * 100)
        assert fake.received == []


async def test_query_larger_than_the_buffer_is_a_clear_error() -> None:
    async with FakeSonic(echo_ok, buffer=40) as fake, client(fake) as s:
        with pytest.raises(ValueError, match="the buffer Sonic announces is 40"):
            await s.query("c", "b", "x" * 100)
        assert fake.received == []


# ---- performance: pool and pipelining -------------------------------------------------------


def slow(delay: float) -> Handler:
    """Like Sonic: PENDING immediately, EVENT after `delay` (several queries at once)."""

    async def h(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
        mid = f"m{len(fake.received)}"
        await fake.say(w, f"PENDING {mid}")
        fake.later(delay, w, f"EVENT QUERY {mid} r")

    return h


async def _burst(s: Sonic, n: int) -> float:
    t = time.perf_counter()
    res = await asyncio.gather(*(s.query("c", "b", f"q{i}") for i in range(n)))
    assert res == [["r"]] * n
    return time.perf_counter() - t


async def test_pipelining_on_a_single_connection() -> None:
    async with FakeSonic(slow(0.1)) as fake, client(fake, pool_size=1) as s:
        dt = await _burst(s, 20)
        assert len(fake.modes) == 1
        assert dt < 0.6  # sequentially it would take 2 s


async def test_without_pipelining_commands_are_serialized() -> None:
    async with FakeSonic(slow(0.1)) as fake, client(fake, pool_size=1, max_in_flight=1) as s:
        dt = await _burst(s, 5)
        assert dt >= 0.5  # 5 x 0.1 s one after another


async def test_pool_opens_lazy_connections_up_to_the_cap() -> None:
    async with FakeSonic(slow(0.1)) as fake, client(fake, pool_size=3, max_in_flight=1) as s:
        assert fake.modes == []  # nothing opens on enter
        dt = await _burst(s, 6)
        assert len(fake.modes) == 3  # exactly pool_size
        assert 0.2 <= dt < 0.5  # 6 commands / 3 connections x 0.1 s


async def test_an_idle_connection_is_reused() -> None:
    async with FakeSonic(echo_ok) as fake, client(fake, pool_size=4) as s:
        for _ in range(5):
            await s.ping()  # sequential: always the same one
        assert len(fake.modes) == 1
