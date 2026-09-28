from collections.abc import Iterator

import pytest

from tests.docker_sonic import PASSWORD, docker_available, sonic_container


@pytest.fixture(scope="session")
def sonic_addr() -> Iterator[tuple[str, int, str]]:
    if not docker_available():
        pytest.skip("SKIPPED: docker is not available; integration tests need a real Sonic")
    with sonic_container() as (host, port):
        yield host, port, PASSWORD
