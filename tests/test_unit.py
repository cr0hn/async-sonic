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
    """Handler minimo: PING->PONG, todo lo demas->OK."""
    await fake.say(w, "PONG" if cmd == "PING" else "OK")


def client(fake: FakeSonic, **kw: int | float | None) -> Sonic:
    return Sonic("127.0.0.1", fake.port, "pw", **kw)  # pyright: ignore[reportArgumentType]


def control_conn(s: Sonic):
    return s._control.conns[0]  # pyright: ignore[reportPrivateUsage]


# ---- escapado: esperados escritos a mano, no derivados de la funcion ----------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("hola", '"hola"'),
        ('di "hola"', '"di \\"hola\\""'),
        ("a\\b", '"a\\\\b"'),
        ("acaba en barra\\", '"acaba en barra\\\\"'),
        ('\\"', '"\\\\\\""'),
        ("linea1\nlinea2", '"linea1 linea2"'),
        ("a\r\nb\rc", '"a b c"'),
        ("canción ñandú 日本 😀", '"canción ñandú 日本 😀"'),
        ("", '""'),
    ],
)
def test_quote(text: str, expected: str) -> None:
    assert quote(text) == expected


# ---- handshake ----------------------------------------------------------------------------


async def test_handshake_manda_start_con_modo_y_lee_el_buffer() -> None:
    async with FakeSonic(echo_ok, buffer=1234) as fake, client(fake) as s:
        await s.ping()
        assert fake.modes == ["control"]  # ping va por control
        assert control_conn(s).buffer == 1234


async def test_password_incorrecto() -> None:
    async with FakeSonic(echo_ok, password="otra") as fake, client(fake) as s:
        with pytest.raises(SonicServerError) as ei:
            await s.ping()
        assert ei.value.code == "authentication_failed"
        assert "channel.auth_password" in str(ei.value)


async def test_saludo_que_no_es_sonic() -> None:
    async with FakeSonic(echo_ok) as fake:
        fake.greeting = "HTTP/1.1 400 Bad Request"
        async with client(fake) as s:
            with pytest.raises(SonicProtocolError, match="no habla el protocolo de Sonic"):
                await s.ping()


async def test_started_sin_buffer() -> None:
    async with FakeSonic(echo_ok) as fake:
        fake.started_suffix = "protocol(1)"
        async with client(fake) as s:
            with pytest.raises(SonicProtocolError, match="START inesperada"):
                await s.ping()


async def test_conexion_rechazada() -> None:
    async with FakeSonic(echo_ok) as fake:
        port = fake.port
    async with Sonic("127.0.0.1", port, "pw") as s:
        with pytest.raises(SonicConnectionError, match="No se pudo conectar") as ei:
            await s.ping()
        assert not isinstance(ei.value, SonicTimeout)


async def test_timeout_de_conexion() -> None:
    async def mudo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await asyncio.sleep(5)  # acepta y nunca saluda

    server = await asyncio.start_server(mudo, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        async with Sonic("127.0.0.1", port, "pw", connect_timeout=0.1) as s:
            with pytest.raises(SonicTimeout, match=r"al conectar"):
                await s.ping()
    finally:
        server.close()


# ---- comandos: linea exacta que sale + parseo de la respuesta -----------------------------


async def test_comandos_de_busqueda() -> None:
    async def h(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
        verb = cmd.split()[0]
        results = {"QUERY": "video:1 video:2", "SUGGEST": "gato gatos", "LIST": ""}
        await fake.say(w, "PENDING m1")
        await fake.say(w, f"EVENT {verb} m1 {results[verb]}")

    async with FakeSonic(h) as fake, client(fake) as s:
        assert await s.query("c", "b", 'di "x"', limit=5, offset=2, lang="spa") == [
            "video:1",
            "video:2",
        ]
        assert await s.query("c", "b", "y") == ["video:1", "video:2"]
        assert await s.suggest("c", "b", "ga", limit=3) == ["gato", "gatos"]
        assert await s.list_words("c", "b", limit=9, offset=1) == []
        assert fake.received == [
            'QUERY c b "di \\"x\\"" LIMIT(5) OFFSET(2) LANG(spa)',
            'QUERY c b "y"',
            'SUGGEST c b "ga" LIMIT(3)',
            "LIST c b LIMIT(9) OFFSET(1)",
        ]
        assert fake.modes == ["search"]


async def test_comandos_de_ingesta() -> None:
    async def h(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
        await fake.say(w, "OK" if cmd.startswith("PUSH") else "RESULT 7")

    async with FakeSonic(h) as fake, client(fake) as s:
        await s.push("c", "b", "o:1", "hola", lang="spa")
        assert await s.pop("c", "b", "o:1", "hola") == 7
        assert await s.count("c") == 7
        assert await s.count("c", "b") == 7
        assert await s.count("c", "b", "o:1") == 7
        assert await s.flush_collection("c") == 7
        assert await s.flush_bucket("c", "b") == 7
        assert await s.flush_object("c", "b", "o:1") == 7
        assert fake.received == [
            'PUSH c b o:1 "hola" LANG(spa)',
            'POP c b o:1 "hola"',
            "COUNT c",
            "COUNT c b",
            "COUNT c b o:1",
            "FLUSHC c",
            "FLUSHB c b",
            "FLUSHO c b o:1",
        ]
        assert fake.modes == ["ingest"]


async def test_comandos_de_control() -> None:
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
    lambda s: s.query("con espacio", "b", "x"),
    lambda s: s.push("c", "b", 'o"1', "x"),
    lambda s: s.query("c", "b", "x", lang="es;"),
    lambda s: s.count("c", None, "obj"),
    lambda s: s.trigger(None, "datos"),
]


@pytest.mark.parametrize("call", CALLS_INVALIDAS)
async def test_argumentos_invalidos_no_salen_al_servidor(
    call: Callable[[Sonic], Awaitable[object]],
) -> None:
    async with FakeSonic(echo_ok) as fake, client(fake) as s:
        with pytest.raises(ValueError):
            await call(s)
        assert fake.received == []


# ---- PENDING / EVENT ----------------------------------------------------------------------


async def test_eventos_desordenados_se_casan_por_id() -> None:
    ids = iter(["idA", "idB"])
    pendientes: dict[str, str] = {}

    async def h(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
        mid = next(ids)
        pendientes[cmd.split('"')[1]] = mid
        await fake.say(w, f"PENDING {mid}")
        if len(pendientes) == 2:  # contesta al reves de como llegaron
            await fake.say(w, f"EVENT QUERY {pendientes['segundo']} res-segundo")
            await fake.say(w, f"EVENT QUERY {pendientes['primero']} res-primero")

    async with FakeSonic(h) as fake, client(fake, pool_size=1) as s:
        a = asyncio.create_task(s.query("c", "b", "primero"))
        await asyncio.sleep(0.05)
        b = asyncio.create_task(s.query("c", "b", "segundo"))
        assert await a == ["res-primero"]
        assert await b == ["res-segundo"]
        assert len(fake.modes) == 1


async def test_event_de_otro_tipo() -> None:
    async def h(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
        await fake.say(w, "PENDING m")
        await fake.say(w, "EVENT SUGGEST m algo")

    async with FakeSonic(h) as fake, client(fake) as s:
        with pytest.raises(SonicProtocolError, match="se esperaba EVENT QUERY"):
            await s.query("c", "b", "x")


async def test_event_sin_pending_cierra_la_conexion() -> None:
    async def h(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
        await fake.say(w, "PENDING m")
        await fake.say(w, "EVENT QUERY otro-id x")

    async with FakeSonic(h) as fake, client(fake) as s:
        with pytest.raises(SonicProtocolError, match="EVENT sin PENDING previo"):
            await s.query("c", "b", "x")


async def test_respuesta_no_solicitada_cierra_la_conexion() -> None:
    async def h(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
        await fake.say(w, "PONG")
        await fake.say(w, "PONG")  # de mas

    async with FakeSonic(h) as fake, client(fake) as s:
        await s.ping()
        await asyncio.sleep(0.05)
        assert control_conn(s).closed


async def test_err_no_rompe_la_conexion() -> None:
    async def h(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
        if cmd.startswith("QUERY"):
            await fake.say(w, "ERR policy_reject(LIMIT out of minimum/maximum bounds)")
        else:
            await fake.say(w, "PONG")

    async with FakeSonic(h) as fake, client(fake) as s:
        with pytest.raises(SonicServerError, match="policy_reject") as ei:
            await s.query("c", "b", "x", limit=0)
        assert ei.value.code == "policy_reject"
        with pytest.raises(SonicServerError):  # la misma conexion sigue alineada
            await s.query("c", "b", "y", limit=0)
        assert len(fake.modes) == 1


# ---- caidas y timeouts --------------------------------------------------------------------


async def test_cierre_abrupto_y_recuperacion() -> None:
    llamadas = 0

    async def h(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
        nonlocal llamadas
        llamadas += 1
        if llamadas == 1:
            w.close()  # corta sin contestar
        else:
            await fake.say(w, "PONG")

    async with FakeSonic(h) as fake, client(fake) as s:
        with pytest.raises(SonicConnectionError, match="Sonic cerro la conexion"):
            await s.ping()
        await s.ping()  # sin reintento magico: este es OTRO comando, y abre conexion nueva
        assert len(fake.modes) == 2


async def test_ended_del_servidor() -> None:
    async def h(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
        await fake.say(w, "ENDED timeout")

    async with FakeSonic(h) as fake, client(fake) as s:
        with pytest.raises(SonicConnectionError, match="ENDED timeout"):
            await s.ping()


async def test_timeout_de_comando_y_respuesta_tardia_no_desincroniza() -> None:
    async def h(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
        if cmd.startswith("QUERY"):
            await fake.say(w, "PENDING lento")
            fake.later(0.3, w, "EVENT QUERY lento tarde")
        else:
            await fake.say(w, "PONG")

    async with FakeSonic(h) as fake, client(fake, timeout=0.1) as s:
        with pytest.raises(SonicTimeout, match=r"no contesto en 0.1s"):
            await s.query("c", "b", "x")
        await asyncio.sleep(0.4)  # llega el EVENT tardio y se descarta
        await s.ping()
        assert fake.modes == ["search", "control"]
        assert not s._search.conns[0].closed  # pyright: ignore[reportPrivateUsage]


async def test_cerrar_manda_quit_y_bloquea_la_conexion() -> None:
    async with FakeSonic(echo_ok) as fake:
        s = client(fake)
        await s.ping()
        conn = control_conn(s)
        await s.close()
        assert fake.received[-1] == "QUIT"
        assert conn.closed
        with pytest.raises(SonicConnectionError, match="Conexion cerrada"):
            await conn.call("PING")


# ---- buffer -------------------------------------------------------------------------------


async def test_push_se_trocea_para_caber_en_el_buffer() -> None:
    texto = " ".join(f'palabra{i}"' for i in range(40)) + " canción\\"
    async with FakeSonic(echo_ok, buffer=80) as fake, client(fake) as s:
        await s.push("c", "b", "o", texto, lang="spa")
        assert len(fake.received) > 3
        palabras: list[str] = []
        for line in fake.received:
            assert len(line.encode()) + 1 <= 80
            assert line.startswith('PUSH c b o "') and line.endswith('" LANG(spa)')
            palabras += line[len('PUSH c b o "') : -len('" LANG(spa)')].split(" ")
        # las palabras llegan enteras y en orden (comillas y barra escapadas)
        esperado = [f'palabra{i}\\"' for i in range(40)] + ["canción\\\\"]
        assert palabras == esperado


async def test_palabra_mayor_que_el_buffer() -> None:
    async with FakeSonic(echo_ok, buffer=40) as fake, client(fake) as s:
        with pytest.raises(ValueError, match="no cabe en el buffer"):
            await s.push("c", "b", "o", "x" * 100)
        assert fake.received == []


async def test_query_mayor_que_el_buffer_es_error_claro() -> None:
    async with FakeSonic(echo_ok, buffer=40) as fake, client(fake) as s:
        with pytest.raises(ValueError, match="el buffer que anuncia Sonic es 40"):
            await s.query("c", "b", "x" * 100)
        assert fake.received == []


# ---- rendimiento: pool y pipelining -------------------------------------------------------


def lento(delay: float) -> Handler:
    """Como Sonic: PENDING al instante, EVENT tras `delay` (varias consultas a la vez)."""

    async def h(cmd: str, fake: FakeSonic, w: asyncio.StreamWriter) -> None:
        mid = f"m{len(fake.received)}"
        await fake.say(w, f"PENDING {mid}")
        fake.later(delay, w, f"EVENT QUERY {mid} r")

    return h


async def _rafaga(s: Sonic, n: int) -> float:
    t = time.perf_counter()
    res = await asyncio.gather(*(s.query("c", "b", f"q{i}") for i in range(n)))
    assert res == [["r"]] * n
    return time.perf_counter() - t


async def test_pipelining_en_una_sola_conexion() -> None:
    async with FakeSonic(lento(0.1)) as fake, client(fake, pool_size=1) as s:
        dt = await _rafaga(s, 20)
        assert len(fake.modes) == 1
        assert dt < 0.6  # en serie serian 2 s


async def test_sin_pipelining_los_comandos_se_serializan() -> None:
    async with FakeSonic(lento(0.1)) as fake, client(fake, pool_size=1, max_in_flight=1) as s:
        dt = await _rafaga(s, 5)
        assert dt >= 0.5  # 5 x 0.1 s uno tras otro


async def test_pool_abre_conexiones_perezosas_hasta_el_tope() -> None:
    async with FakeSonic(lento(0.1)) as fake, client(fake, pool_size=3, max_in_flight=1) as s:
        assert fake.modes == []  # nada se abre al entrar
        dt = await _rafaga(s, 6)
        assert len(fake.modes) == 3  # ni mas ni menos que pool_size
        assert 0.2 <= dt < 0.5  # 6 comandos / 3 conexiones x 0.1 s


async def test_una_conexion_ociosa_se_reutiliza() -> None:
    async with FakeSonic(echo_ok) as fake, client(fake, pool_size=4) as s:
        for _ in range(5):
            await s.ping()  # secuencial: siempre la misma
        assert len(fake.modes) == 1
