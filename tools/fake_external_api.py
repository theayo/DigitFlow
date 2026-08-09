"""A stand-in for the external file API, for manual testing.

Development tool, not part of the service: nothing in `app/` imports it and it is
never started by `docker-compose.yml`. It exists because the download page cannot
honestly be checked against the real service — the identifier is single-use, and
once its catalog is downloaded `/names` returns empty forever (see PROJECT.md
§ 2). Against this stub the whole cycle can be replayed as often as needed, with
pauses and failures armed on demand.

Standard library only, so it runs inside the existing `api` image without adding
a dependency or a service.

    # inside the api container, reachable from the worker as http://api:9000
    python tools/fake_external_api.py --catalog 12

Contract-compatible with the real service on purpose, including the parts the
client validates strictly (PROJECT.md § 8.0): `/names` answers with a
`file_names` list, `/downloaded` answers with two counters whose sum equals the
number of names sent, and `/download` refuses more than three names.

Control endpoints, all outside `/api/`:

    GET  /_state                       what is left, what is marked
    POST /_reset                       fresh catalog, nothing marked
    POST /_fault {"status": 429, "retry_after": 30, "count": 1, "path": "names"}
                                       arm the next answers to fail

`POST /_fault` is what makes the waiting state visible: arm a 403 with
`retry_after` and watch the run switch to `waiting_retry` and count down.
"""

import argparse
import hashlib
import io
import json
import re
import zipfile
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from random import Random
from threading import Lock

CONTENT_LENGTH = 500
MAX_NAMES_PER_DOWNLOAD = 3
NAMES_MIN, NAMES_MAX = 3, 9

NAMES_PATH = "/api/files/names"
DOWNLOAD_PATH = "/api/files/download"
DOWNLOADED_PATH = "/api/files/downloaded"


def content_for(name: str) -> str:
    """Deterministic 500 digits for a name.

    Deterministic on purpose: asking for the same file twice has to return the
    same bytes, otherwise the conflict check in § 8.3 would fire on a second run
    and report corruption that only the stub invented.
    """
    digits: list[str] = []
    seed = name.encode()
    while len(digits) < CONTENT_LENGTH:
        seed = hashlib.sha256(seed).digest()
        digits.extend(str(byte % 10) for byte in seed)
    return "".join(digits[:CONTENT_LENGTH])


def zip_for(names: list[str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name in names:
            archive.writestr(name, content_for(name))
    return buffer.getvalue()


@dataclass
class Fault:
    """An answer to fail with, armed ahead of time."""

    status: int
    retry_after: int | None = None
    count: int = 1
    path: str = "any"  # "any" | "names" | "download" | "downloaded"

    def matches(self, path: str) -> bool:
        return self.path in ("any", path)


@dataclass
class Catalog:
    """The whole state of the stub: what exists, what each candidate marked."""

    size: int
    missing: set[str] = field(default_factory=set)
    marked: dict[str, set[str]] = field(default_factory=dict)
    faults: list[Fault] = field(default_factory=list)
    lock: Lock = field(default_factory=Lock)
    random: Random = field(default_factory=lambda: Random(20260809))

    @property
    def names(self) -> list[str]:
        return [f"stub-{index:04d}.txt" for index in range(1, self.size + 1)]

    def remaining(self, candidate: str) -> list[str]:
        return [name for name in self.names if name not in self.marked.get(candidate, set())]

    def take_fault(self, path: str) -> Fault | None:
        for fault in self.faults:
            if not fault.matches(path):
                continue
            fault.count -= 1
            if fault.count <= 0:
                self.faults.remove(fault)
            return fault
        return None


class Handler(BaseHTTPRequestHandler):
    catalog: Catalog

    server_version = "FakeFilesAPI/1.0"

    # --- plumbing -----------------------------------------------------------

    def log_message(self, format: str, *args: object) -> None:
        print(f"  {self.command} {self.path} -> {args[1] if len(args) > 1 else ''}", flush=True)

    def _json(self, status: int, payload: object, headers: dict[str, str] | None = None) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def _bytes(self, payload: bytes) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "application/zip")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        try:
            return json.loads(self.rfile.read(length))
        except ValueError:
            return {}

    def _candidate(self) -> str:
        # The real service falls back to the client IP; the exact value does not
        # matter here, only that different identifiers keep separate progress.
        return self.headers.get("X-Candidate-Id") or "by-ip"

    def _armed(self, path: str) -> bool:
        """Answer with an armed failure, if one is due. True when it fired."""
        with self.catalog.lock:
            fault = self.catalog.take_fault(path)
        if fault is None:
            return False

        headers = {}
        if fault.retry_after is not None:
            headers["Retry-After"] = str(fault.retry_after)
        self._json(fault.status, {"detail": f"подстроенный сбой {fault.status}"}, headers)
        return True

    # --- routes -------------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802 — BaseHTTPRequestHandler's naming
        if self.path.startswith("/_state"):
            with self.catalog.lock:
                self._json(
                    200,
                    {
                        "catalog": self.catalog.size,
                        "missing": sorted(self.catalog.missing),
                        "marked": {
                            candidate: len(names)
                            for candidate, names in self.catalog.marked.items()
                        },
                        "faults": [vars(fault) for fault in self.catalog.faults],
                    },
                )
            return

        if self.path.split("?")[0] != NAMES_PATH:
            self._json(404, {"detail": "нет такой ручки"})
            return

        if self._armed("names"):
            return

        candidate = self._candidate()
        with self.catalog.lock:
            remaining = self.catalog.remaining(candidate)
            portion = self.catalog.random.randint(NAMES_MIN, NAMES_MAX)
            names = remaining[:portion]
        self._json(200, {"file_names": names})

    def do_POST(self) -> None:  # noqa: N802
        path = self.path.split("?")[0]

        if path == "/_reset":
            with self.catalog.lock:
                self.catalog.marked.clear()
                self.catalog.faults.clear()
            self._json(200, {"detail": "каталог сброшен"})
            return

        if path == "/_fault":
            body = self._body()
            fault = Fault(
                status=int(body.get("status", 429)),
                retry_after=body.get("retry_after"),
                count=int(body.get("count", 1)),
                path=str(body.get("path", "any")),
            )
            with self.catalog.lock:
                self.catalog.faults.append(fault)
            self._json(200, {"armed": vars(fault)})
            return

        if path == DOWNLOAD_PATH:
            self._download()
            return

        if path == DOWNLOADED_PATH:
            self._downloaded()
            return

        self._json(404, {"detail": "нет такой ручки"})

    def _download(self) -> None:
        if self._armed("download"):
            return

        names = self._body().get("file_names")
        if not isinstance(names, list) or not 1 <= len(names) <= MAX_NAMES_PER_DOWNLOAD:
            self._json(422, {"detail": f"от 1 до {MAX_NAMES_PER_DOWNLOAD} имён"})
            return

        with self.catalog.lock:
            known = set(self.catalog.names) - self.catalog.missing
        unknown = [name for name in names if name not in known]
        if unknown:
            # Deliberately unhelpful, exactly like the real service: the client is
            # told something is missing but not what (§ 8.4).
            self._json(404, {"detail": "какого-то файла нет в каталоге"})
            return

        self._bytes(zip_for(names))

    def _downloaded(self) -> None:
        if self._armed("downloaded"):
            return

        names = self._body().get("file_names")
        if not isinstance(names, list) or not names:
            self._json(422, {"detail": "нужен непустой список имён"})
            return

        candidate = self._candidate()
        with self.catalog.lock:
            known = set(self.catalog.names)
            if any(name not in known for name in names):
                self._json(404, {"detail": "какого-то файла нет в каталоге"})
                return

            already = self.catalog.marked.setdefault(candidate, set())
            marked_now = sum(1 for name in names if name not in already)
            already.update(names)

        self._json(200, {"marked_now": marked_now, "already_marked": len(names) - marked_now})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="0.0.0.0")  # noqa: S104 — container-local
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--catalog", type=int, default=12, help="сколько файлов в каталоге")
    parser.add_argument(
        "--missing",
        default="",
        help="имена, которых 'нет в каталоге': отдают 404 на /download (через запятую). "
        "Принимает и номер: 3 означает stub-0003.txt",
    )
    arguments = parser.parse_args()

    catalog = Catalog(size=arguments.catalog)
    for raw in filter(None, (item.strip() for item in arguments.missing.split(","))):
        name = f"stub-{int(raw):04d}.txt" if re.fullmatch(r"\d+", raw) else raw
        catalog.missing.add(name)

    Handler.catalog = catalog
    server = ThreadingHTTPServer((arguments.host, arguments.port), Handler)

    print(
        f"заглушка внешнего API на {arguments.host}:{arguments.port}, "
        f"каталог {catalog.size} файлов"
        + (f", отсутствуют {sorted(catalog.missing)}" if catalog.missing else ""),
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("остановлена", flush=True)


if __name__ == "__main__":
    main()
