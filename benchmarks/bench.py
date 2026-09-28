"""Compara 1 conexion secuencial, pool concurrente y pipelining contra un Sonic real.

uv run python benchmarks/bench.py            # levanta Sonic en docker
SONIC_ADDR=host:1491 SONIC_PASSWORD=... uv run python benchmarks/bench.py   # o uno existente
"""

import asyncio
import os
import sys
import time
from collections.abc import Awaitable, Callable
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from async_sonic import Sonic
from tests.docker_sonic import PASSWORD, sonic_container

N = 2000
COL, BUCKET = "bench", "b"


async def cronometrar(
    nombre: str,
    addr: tuple[str, int, str],
    op: Callable[[Sonic, int], Awaitable[object]],
    **kw: object,
) -> None:
    host, port, pw = addr
    # secuencial <=> conc == 1
    conc = int(kw.pop("conc", N))  # pyright: ignore[reportArgumentType]
    async with Sonic(host, port, pw, **kw) as s:  # pyright: ignore[reportArgumentType]
        await s.ping()
        t = time.perf_counter()
        if conc == 1:
            for i in range(N):
                await op(s, i)
        else:
            await asyncio.gather(*(op(s, i) for i in range(N)))
        dt = time.perf_counter() - t
    print(f"  {nombre:<44} {dt:7.3f} s  {N / dt:9.0f} ops/s")


async def main(addr: tuple[str, int, str]) -> None:
    host, port, pw = addr
    async with Sonic(host, port, pw) as s:
        for i in range(500):
            await s.push(COL, BUCKET, f"o{i}", f"documento{i} texto comun", lang="eng")
    q = lambda s, i: s.query(COL, BUCKET, f"documento{i % 500}", lang="eng")  # noqa: E731
    p = lambda s, i: s.push(COL, BUCKET, f"p{i}", f"empuje{i} texto", lang="eng")  # noqa: E731
    for titulo, op in (("QUERY", q), ("PUSH", p)):
        print(f"{titulo} x {N}")
        await cronometrar("1 conexion, secuencial", addr, op, pool_size=1, conc=1)
        await cronometrar(
            "pool 8, concurrente, sin pipelining", addr, op, pool_size=8, max_in_flight=1
        )
        await cronometrar("1 conexion, pipelining", addr, op, pool_size=1)
        await cronometrar("pool 8 + pipelining", addr, op, pool_size=8)


if os.environ.get("SONIC_ADDR"):
    h, _, pt = os.environ["SONIC_ADDR"].rpartition(":")
    asyncio.run(main((h, int(pt), os.environ.get("SONIC_PASSWORD", PASSWORD))))
else:
    with sonic_container() as (h, pt):
        asyncio.run(main((h, pt, PASSWORD)))
