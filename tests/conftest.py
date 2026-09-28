from collections.abc import Iterator

import pytest

from tests.docker_sonic import PASSWORD, docker_disponible, sonic_container


@pytest.fixture(scope="session")
def sonic_addr() -> Iterator[tuple[str, int, str]]:
    if not docker_disponible():
        pytest.skip("SALTADO: no hay docker; los tests de integracion necesitan un Sonic real")
    with sonic_container() as (host, port):
        yield host, port, PASSWORD
