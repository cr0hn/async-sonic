# async-sonic

Cliente **asyncio** de [Sonic](https://github.com/valeriansaliou/sonic) (el buscador
de identificadores en Rust), en un solo módulo, **sin dependencias** y con tipado
estricto (`py.typed`). Requiere Python 3.14 o superior. Licencia MIT.

Referencia compacta para LLMs y para consulta rápida: [`llms.txt`](llms.txt).

## Inicio rápido

<!-- inicio-rapido -->
```python
import asyncio
from async_sonic import Sonic


async def main() -> None:
    async with Sonic("localhost", 1491, "SecretPassword") as sonic:
        await sonic.push("videos", "catalogo", "video:1", "gatos y perros graciosos", lang="spa")
        await sonic.push("videos", "catalogo", "video:2", "perros callejeros", lang="spa")
        print(await sonic.query("videos", "catalogo", "gatos", lang="spa"))
        await sonic.trigger(
            "consolidate"
        )  # SUGGEST lee un grafo que solo se actualiza al consolidar
        print(await sonic.suggest("videos", "catalogo", "grac"))
        await sonic.flush_collection("videos")


asyncio.run(main())
```
Salida:
```text
['video:1']
['graciosos']
```
<!-- /inicio-rapido -->

Este bloque se ejecuta como test contra un Sonic real
(`tests/test_integration.py::test_readme_inicio_rapido_se_ejecuta`): si miente, la
suite falla.

## Por qué existe

El cliente asyncio que había (`asonic`, última versión de 2020) no funciona. El
resto de clientes de PyPI (`sonic-client`, `pysonic-channel`) son síncronos y hay que
envolverlos en hilos. Esto es el protocolo de texto de Sonic (`PROTOCOL.md`)
escrito una vez, con `asyncio.open_connection` y nada más: sin dependencias que
mantener, pool y pipelining incluidos.

## Instalación

```bash
uv add async-sonic     # o: pip install async-sonic
```

Desde el repo: `uv sync`.

## Una clase, métodos planos

`Sonic` esconde el handshake, los tres canales (search, ingest, control) y el pool:
cada método va solo al canal que toca. Sonic no se toca al construir ni al entrar en
el `async with`; las conexiones se abren perezosamente en el primer comando.

```python
Sonic(host="localhost", port=1491, password="", *,
      pool_size=4, max_in_flight=None, timeout=10.0, connect_timeout=5.0)
```

| Método | Comando Sonic | Canal | Devuelve |
|---|---|---|---|
| `query(coleccion, bucket, terms, *, limit, offset, lang)` | `QUERY` | search | `list[str]` ids, mejor primero |
| `suggest(coleccion, bucket, word, *, limit)` | `SUGGEST` | search | `list[str]` palabras |
| `list_words(coleccion, bucket, *, limit, offset)` | `LIST` | search | `list[str]` palabras |
| `push(coleccion, bucket, object, text, *, lang)` | `PUSH` | ingest | `None` |
| `pop(coleccion, bucket, object, text)` | `POP` | ingest | `int` |
| `count(coleccion, bucket=None, object=None)` | `COUNT` | ingest | `int` |
| `flush_collection(coleccion)` | `FLUSHC` | ingest | `int` |
| `flush_bucket(coleccion, bucket)` | `FLUSHB` | ingest | `int` |
| `flush_object(coleccion, bucket, object)` | `FLUSHO` | ingest | `int` |
| `trigger(action=None, data=None)` | `TRIGGER` | control | `str` |
| `info()` | `INFO` | control | `dict[str, int]` |
| `ping()` | `PING` | control | `None` |
| `help(manual=None)` | `HELP` | control | `str` |

`close()` (o salir del `async with`) manda `QUIT` a cada conexión. `lang` es un
código ISO 639-3 (`"spa"`) o `"none"`; sin él, Sonic adivina el idioma del texto (y
puede adivinar distinto al indexar y al buscar: pásalo en los dos lados).

Nombres que difieren del protocolo, a propósito:
- `list_words`: `LIST` enumera **palabras** del índice, no objetos.
- `count` usa `COUNT`, no `COUNTC/COUNTB/COUNTO`: `PROTOCOL.md` los documenta, pero
  Sonic v1.9.1 contesta `ERR unknown_command`. Además `count(col, bucket)` devuelve
  en v1.9.1 palabras distintas, no objetos (medido: 2 objetos, 4 palabras, 4).

## Conexiones, pool y pipelining

Un canal Sonic es una conexión TCP en un modo. Por cada modo hay un pool:

- **Pool**: hasta `pool_size` conexiones, abiertas cuando hacen falta (una ociosa se
  reutiliza antes de abrir otra). Un comando va a la conexión menos cargada.
- **Pipelining**: cada conexión escribe sin esperar respuesta y tiene una tarea
  lectora que las reparte a futuros. `PROTOCOL.md` lo permite y lo ejemplifica
  (T11-T18): las respuestas inmediatas (`OK`, `RESULT`, `PONG`, `PENDING <id>`, `ERR`)
  llegan **en orden** (cola FIFO), y los `EVENT` de `QUERY`/`SUGGEST`/`LIST` llegan
  **desordenados** y se casan por el id de `PENDING`. `max_in_flight` limita los
  comandos simultáneos por conexión (`None` = sin tope; `1` = sin pipelining).
- **Sin reintentos, sin reconexión mágica.** Si una conexión se cae, sus comandos en
  vuelo fallan con `SonicConnectionError`; el **siguiente** comando abre una
  conexión nueva. Lo que reintentar, y cuándo, lo decides tú (un `PUSH` es
  idempotente si haces antes `flush_object`; un `POP`, no).
- **Timeout por comando** (`timeout`): lanza `SonicTimeout`, pero la conexión sigue
  viva; la respuesta tardía se descarta al llegar y no desincroniza nada. Timeout de
  conexión: `connect_timeout`.

### Rendimiento medido

`python benchmarks/bench.py` (Apple M2 Max, Sonic v1.9.1 en Docker Desktop, 2000
operaciones por fila, una sola ejecución en localhost: el orden de magnitud sirve,
los decimales no):

| Escenario | QUERY ops/s | PUSH ops/s |
|---|---:|---:|
| 1 conexión, secuencial | 2216 | 3305 |
| pool 8, concurrente, sin pipelining | 9874 | 11836 |
| 1 conexión, pipelining | 12371 | 21067 |
| pool 8 + pipelining | 16958 | 19189 |

El pipelining es la palanca grande; el pool suma en consultas (Sonic las reparte en
hilos) y no en escrituras (una conexión ya satura el ingest). En red real con
latencia el salto del secuencial es mucho mayor, porque cada ida y vuelta se paga
una sola vez por ráfaga.

## Errores

Todos heredan de `SonicError` y el mensaje dice qué pasó y qué hacer.

| Excepción | Cuándo |
|---|---|
| `SonicConnectionError` | no se conectó, conexión caída o cerrada, `ENDED` del servidor |
| `SonicTimeout` (hija de la anterior) | venció `connect_timeout` o `timeout` |
| `SonicServerError` | Sonic contestó `ERR ...` (`.code`, `.line`); también password incorrecto (`authentication_failed`) |
| `SonicProtocolError` | Sonic dijo algo fuera de `PROTOCOL.md`; la conexión se cierra |
| `ValueError` (estándar) | argumento inválido o comando que no cabe en el buffer; **no sale al servidor** |

## Escapado y límites

`quote(text)` (público) envuelve el texto en comillas: `"` pasa a `\"`, `\` a `\\`
(para que una barra final no se coma la comilla de cierre) y los saltos de línea
(`\n`, `\r`) a espacio, porque un salto crudo cortaría el comando; Sonic tokeniza por
espacios, así que no se pierde ninguna palabra. Unicode va tal cual en UTF-8.
`collection`, `bucket`, `object`, `action`… no pueden estar vacíos ni llevar
espacios, comillas o caracteres de control (`ValueError`).

**Buffer.** `STARTED ... buffer(N)` (20000 por defecto) es el tope de una línea
entera, salto incluido. `PROTOCOL.md` pide trocear: `push` lo hace solo, en varios
`PUSH` del mismo objeto cortando **entre palabras**. `query`, `suggest` y `pop` no se
trocean (partirlas cambiaría lo que significan): lanzan `ValueError`. Una palabra
suelta más larga que el buffer también.

## Tests

```bash
uv sync
uv run pytest                    # todo
uv run pytest tests/test_unit.py # sin docker
uv run ruff check . && uv run ruff format --check . && uv run pyright
```

- `tests/test_unit.py`: contra un servidor falso asyncio (`tests/fake_sonic.py`) que
  habla el protocolo real: handshake, `PENDING`/`EVENT` desordenados, `ERR`, cierre
  abrupto, `ENDED`, respuestas lentas (timeout y respuesta tardía), troceado por
  buffer, pool y pipelining con latencia.
- `tests/test_integration.py`: contra un **Sonic real**. Si hay `docker`, la fixture
  levanta `valeriansaliou/sonic:v1.9.1` con `tests/sonic.cfg` (password
  `SecretPassword`) en un puerto libre. **Sin docker estos tests se saltan con
  motivo** (`SALTADO: no hay docker...`): mira el número de `skipped` del informe;
  un verde con todos los de integración saltados no prueba el cableado real.

## Compatibilidad

- **Verificado**: Sonic **v1.9.1** (imagen oficial `valeriansaliou/sonic:v1.9.1`) en
  Docker Desktop, macOS arm64, Python 3.14.7: todos los comandos de la tabla, texto con
  comillas, barras, saltos de línea, acentos, CJK y emoji, texto de 48 KB (troceado),
  `ERR` reales, password incorrecto, y 200 consultas concurrentes con pool y pipelining.
- **No verificado**: otras versiones de Sonic (`COUNTC/B/O` puede existir en las
  posteriores); `TRIGGER backup`/`restore` (se envían, no se probaron contra datos
  reales); TLS o proxies (Sonic Channel es TCP plano); Windows/Linux; carga sostenida
  o servidor con muchos clientes; el comportamiento con `tcp_timeout` vencido más allá
  de que se detecta como `ENDED`.
- La búsqueda con acentos sin acento (`cancion` → `canción`) depende de la
  configuración del servidor (`diacritic_folding_enabled`), no de este cliente; el
  `sonic.cfg` de pruebas no la activa.
