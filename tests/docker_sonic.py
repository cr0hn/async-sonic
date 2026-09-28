"""Start `valeriansaliou/sonic` in docker with tests/sonic.cfg. Used by the integration
tests and by benchmarks/bench.py."""

import contextlib
import shutil
import socket
import subprocess
import time
from collections.abc import Generator
from pathlib import Path

IMAGE = "valeriansaliou/sonic:v1.9.1"
PASSWORD = "SecretPassword"
CFG = Path(__file__).parent / "sonic.cfg"


def docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    return subprocess.run(["docker", "info"], capture_output=True).returncode == 0


@contextlib.contextmanager
def sonic_container() -> Generator[tuple[str, int]]:
    cid = subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "-p",
            "127.0.0.1::1491",
            "-v",
            f"{CFG}:/etc/sonic.cfg",
            IMAGE,
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    try:
        out = subprocess.run(
            ["docker", "port", cid, "1491/tcp"], capture_output=True, text=True, check=True
        )
        port = int(out.stdout.splitlines()[0].rsplit(":", 1)[1])
        deadline = time.time() + 30
        while True:  # wait for the greeting, not just for the port to open
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1) as s:
                    if s.recv(64).startswith(b"CONNECTED"):
                        break
            except OSError:
                pass
            if time.time() > deadline:
                raise RuntimeError("Sonic did not start within 30 s")
            time.sleep(0.2)
        yield "127.0.0.1", port
    finally:
        subprocess.run(["docker", "rm", "-f", cid], capture_output=True)
