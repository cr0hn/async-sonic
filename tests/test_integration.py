"""Against a REAL Sonic (valeriansaliou/sonic in docker). Without docker they skip, with a reason."""

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
    name = str(request.node.name)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
    return "t" + re.sub(r"\W", "", name)[:40]  # one collection per test


async def test_full_flow(sonic: Sonic, col: str) -> None:
    await sonic.push(col, "b", "video:1", "cats and dogs are funny", lang="eng")
    await sonic.push(col, "b", "video:2", "stray dogs", lang="eng")
    assert await sonic.query(col, "b", "cats", lang="eng") == ["video:1"]
    assert sorted(await sonic.query(col, "b", "dogs", lang="eng")) == ["video:1", "video:2"]
    assert await sonic.query(col, "b", "dogs", limit=1, lang="eng") in (["video:1"], ["video:2"])
    assert await sonic.query(col, "b", "nothinglikethis", lang="spa") == []
    await sonic.trigger("consolidate")  # SUGGEST reads the FST graph, updated only on consolidate
    assert "funny" in await sonic.suggest(col, "b", "fun")
    assert "stray" in await sonic.list_words(col, "b")
    # COUNT <collection> <bucket> on v1.9.1 counts distinct words (4 here), not objects (2) as PROTOCOL.md says
    assert await sonic.count(col, "b") == 4
    assert await sonic.count(col) == 1
    assert await sonic.count(col, "b", "video:1") > 0
    assert await sonic.pop(col, "b", "video:1", "cats") == 1
    assert await sonic.query(col, "b", "cats", lang="eng") == []
    assert await sonic.flush_object(col, "b", "video:2") > 0
    assert await sonic.query(col, "b", "dogs", lang="eng") == ["video:1"]
    assert await sonic.flush_bucket(col, "b") > 0
    assert await sonic.count(col, "b") == 0
    await sonic.push(col, "b2", "o", "something", lang="eng")
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
    "text",
    [
        'he said "hello world" and left',
        "path C:\\data\\new",
        "ends with backslash\\",
        "first line\nsecond line table",
        "canción camión ñandú",
        "日本語 テスト",
        "emoji 😀 party",
    ],
)
async def test_hard_texts_are_accepted_and_do_not_desync(sonic: Sonic, col: str, text: str) -> None:
    await sonic.push(col, "b", "o1", text, lang="none")
    assert await sonic.count(col, "b", "o1") > 0
    await sonic.ping()  # the channel is still in sync after the odd text
    await sonic.push(col, "b", "o2", "control", lang="eng")
    assert await sonic.query(col, "b", "control", lang="eng") == ["o2"]


async def test_accents_and_quotes_are_found(sonic: Sonic, col: str) -> None:
    await sonic.push(col, "b", "o1", 'the "canción" of the camión', lang="spa")
    assert await sonic.query(col, "b", "canción", lang="spa") == ["o1"]
    assert await sonic.query(col, "b", '"camión"', lang="spa") == ["o1"]  # quotes in the query


async def test_text_larger_than_the_buffer_is_split(sonic: Sonic, col: str) -> None:
    words = [f"word{i:05d}" for i in range(4000)]  # ~48 KB > buffer(20000)
    await sonic.push(col, "b", "big", " ".join(words), lang="eng")
    assert await sonic.query(col, "b", "word00000", lang="eng") == ["big"]
    assert await sonic.query(col, "b", "word03999", lang="eng") == ["big"]


async def test_real_server_error(sonic: Sonic, col: str) -> None:
    with pytest.raises(SonicServerError) as ei:
        await sonic.query(col, "b", "x", limit=0)
    assert ei.value.code == "policy_reject"
    with pytest.raises(SonicServerError) as ei2:
        await sonic.trigger("doesnotexist")
    assert ei2.value.code == "not_found"
    await sonic.ping()


async def test_real_wrong_password(sonic_addr: tuple[str, int, str]) -> None:
    host, port, _ = sonic_addr
    async with Sonic(host, port, "wrong") as s:
        with pytest.raises(SonicServerError) as ei:
            await s.ping()
        assert ei.value.code == "authentication_failed"


async def test_real_timeout(sonic_addr: tuple[str, int, str]) -> None:
    host, port, pw = sonic_addr
    async with Sonic(host, port, pw, timeout=0.000001) as s:
        with pytest.raises(SonicTimeout):
            await s.ping()


async def test_real_concurrency_with_pool_and_pipelining(
    sonic_addr: tuple[str, int, str], col: str
) -> None:
    host, port, pw = sonic_addr
    async with Sonic(host, port, pw, pool_size=4) as s:
        await asyncio.gather(
            *(s.push(col, "b", f"o{i}", f"document{i} common", lang="eng") for i in range(200))
        )
        res = await asyncio.gather(
            *(s.query(col, "b", f"document{i}", lang="eng") for i in range(200))
        )
        assert res == [[f"o{i}"] for i in range(200)]  # every query gets ITS OWN answer
        assert 1 < len(s._search.conns) <= 4  # pyright: ignore[reportPrivateUsage]
        assert len(s._ingest.conns) <= 4  # pyright: ignore[reportPrivateUsage]


async def test_readme_quickstart_runs(
    sonic_addr: tuple[str, int, str], capfd: pytest.CaptureFixture[str]
) -> None:
    text = README.read_text()
    m = re.search(r"<!-- quickstart -->\s*```python\n(.*?)```(.*?)<!-- /quickstart -->", text, re.S)
    assert m, "README must carry the block between <!-- quickstart --> ... <!-- /quickstart -->"
    code = m.group(1)
    expected = re.search(r"```text\n(.*?)```", m.group(2), re.S)
    assert expected, "the code is followed by a ```text block with the expected output"
    host, port, pw = sonic_addr
    code = code.replace("1491", str(port)).replace("localhost", host).replace("SecretPassword", pw)
    await asyncio.to_thread(exec, compile(code, "README.md", "exec"), {"__name__": "__main__"})
    assert capfd.readouterr().out == expected.group(1)
