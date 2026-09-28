"""Contra un Sonic REAL (valeriansaliou/sonic en docker). Sin docker se saltan, con motivo."""

import asyncio
import re
from pathlib import Path

import pytest

from async_sonic import Sonic, SonicServerError, SonicTimeout

pytestmark = pytest.mark.integration

README = Path(__file__).parent.parent / "README.md"


@pytest.fixture
async def sonic(sonic_addr: tuple[str, int, str], request: pytest.FixtureRequest):
    host, port, pw = sonic_addr
    async with Sonic(host, port, pw) as s:
        yield s


@pytest.fixture
def col(request: pytest.FixtureRequest) -> str:
    return "t" + re.sub(r"\W", "", request.node.name)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType][:40]  # coleccion propia por test


async def test_flujo_completo(sonic: Sonic, col: str) -> None:
    await sonic.push(col, "b", "video:1", "gatos y perros graciosos", lang="spa")
    await sonic.push(col, "b", "video:2", "perros callejeros", lang="spa")
    assert await sonic.query(col, "b", "gatos", lang="spa") == ["video:1"]
    assert sorted(await sonic.query(col, "b", "perros", lang="spa")) == ["video:1", "video:2"]
    assert await sonic.query(col, "b", "perros", limit=1, lang="spa") in (["video:1"], ["video:2"])
    assert await sonic.query(col, "b", "nadadeesto", lang="spa") == []
    await sonic.trigger(
        "consolidate"
    )  # SUGGEST lee el grafo FST, que solo se actualiza al consolidar
    assert "graciosos" in await sonic.suggest(col, "b", "grac")
    assert "callejeros" in await sonic.list_words(col, "b")
    # COUNT <col> <bucket> en v1.9.1 cuenta palabras distintas (4 aqui), no objetos (2) como dice PROTOCOL.md
    assert await sonic.count(col, "b") == 4
    assert await sonic.count(col) == 1
    assert await sonic.count(col, "b", "video:1") > 0
    assert await sonic.pop(col, "b", "video:1", "gatos") == 1
    assert await sonic.query(col, "b", "gatos", lang="spa") == []
    assert await sonic.flush_object(col, "b", "video:2") > 0
    assert await sonic.query(col, "b", "perros", lang="spa") == ["video:1"]
    assert await sonic.flush_bucket(col, "b") > 0
    assert await sonic.count(col, "b") == 0
    await sonic.push(col, "b2", "o", "algo", lang="spa")
    assert await sonic.flush_collection(col) == 1
    assert await sonic.count(col) == 0


async def test_control(sonic: Sonic) -> None:
    await sonic.ping()
    assert await sonic.trigger("consolidate") == ""
    assert await sonic.trigger() == "actions(consolidate, backup, restore)"
    info = await sonic.info()
    assert info["clients_connected"] >= 1 and "uptime" in info
    assert "commands" in await sonic.help()


@pytest.mark.parametrize(
    "texto",
    [
        'dijo "hola mundo" y se fue',
        "ruta C:\\datos\\nuevo",
        "acaba con barra\\",
        "primera linea\nsegunda linea tabla",
        "canción camión ñandú",
        "日本語 テスト",
        "emoji 😀 fiesta",
    ],
)
async def test_textos_dificiles_se_aceptan_y_no_desincronizan(
    sonic: Sonic, col: str, texto: str
) -> None:
    await sonic.push(col, "b", "o1", texto, lang="none")
    assert await sonic.count(col, "b", "o1") > 0
    await sonic.ping()  # el canal sigue alineado tras el texto raro
    await sonic.push(col, "b", "o2", "control", lang="eng")
    assert await sonic.query(col, "b", "control", lang="eng") == ["o2"]


async def test_acentos_y_comillas_se_encuentran(sonic: Sonic, col: str) -> None:
    await sonic.push(col, "b", "o1", 'la "canción" del camión', lang="spa")
    assert await sonic.query(col, "b", "canción", lang="spa") == ["o1"]
    assert await sonic.query(col, "b", '"camión"', lang="spa") == [
        "o1"
    ]  # con comillas en la consulta


async def test_texto_mayor_que_el_buffer_se_trocea(sonic: Sonic, col: str) -> None:
    palabras = [f"palabra{i:05d}" for i in range(4000)]  # ~48 KB > buffer(20000)
    await sonic.push(col, "b", "grande", " ".join(palabras), lang="eng")
    assert await sonic.query(col, "b", "palabra00000", lang="eng") == ["grande"]
    assert await sonic.query(col, "b", "palabra03999", lang="eng") == ["grande"]


async def test_error_real_del_servidor(sonic: Sonic, col: str) -> None:
    with pytest.raises(SonicServerError) as ei:
        await sonic.query(col, "b", "x", limit=0)
    assert ei.value.code == "policy_reject"
    with pytest.raises(SonicServerError) as ei2:
        await sonic.trigger("noexiste")
    assert ei2.value.code == "not_found"
    await sonic.ping()


async def test_password_incorrecto_real(sonic_addr: tuple[str, int, str]) -> None:
    host, port, _ = sonic_addr
    async with Sonic(host, port, "mal") as s:
        with pytest.raises(SonicServerError) as ei:
            await s.ping()
        assert ei.value.code == "authentication_failed"


async def test_timeout_real(sonic_addr: tuple[str, int, str]) -> None:
    host, port, pw = sonic_addr
    async with Sonic(host, port, pw, timeout=0.000001) as s:
        with pytest.raises(SonicTimeout):
            await s.ping()


async def test_concurrencia_real_con_pool_y_pipelining(
    sonic_addr: tuple[str, int, str], col: str
) -> None:
    host, port, pw = sonic_addr
    async with Sonic(host, port, pw, pool_size=4) as s:
        await asyncio.gather(
            *(s.push(col, "b", f"o{i}", f"documento{i} comun", lang="eng") for i in range(200))
        )
        res = await asyncio.gather(
            *(s.query(col, "b", f"documento{i}", lang="eng") for i in range(200))
        )
        assert res == [[f"o{i}"] for i in range(200)]  # cada consulta recibe SU respuesta
        assert 1 < len(s._search.conns) <= 4  # pyright: ignore[reportPrivateUsage]
        assert len(s._ingest.conns) <= 4  # pyright: ignore[reportPrivateUsage]


async def test_readme_inicio_rapido_se_ejecuta(
    sonic_addr: tuple[str, int, str], capfd: pytest.CaptureFixture[str]
) -> None:
    text = README.read_text()
    m = re.search(
        r"<!-- inicio-rapido -->\s*```python\n(.*?)```(.*?)<!-- /inicio-rapido -->", text, re.S
    )
    assert m, (
        "el README debe llevar el bloque entre <!-- inicio-rapido --> ... <!-- /inicio-rapido -->"
    )
    code = m.group(1)
    esperado = re.search(r"```text\n(.*?)```", m.group(2), re.S)
    assert esperado, "tras el codigo va un bloque ```text con la salida esperada"
    host, port, pw = sonic_addr
    code = code.replace("1491", str(port)).replace("localhost", host).replace("SecretPassword", pw)
    await asyncio.to_thread(exec, compile(code, "README.md", "exec"), {"__name__": "__main__"})
    assert capfd.readouterr().out == esperado.group(1)
