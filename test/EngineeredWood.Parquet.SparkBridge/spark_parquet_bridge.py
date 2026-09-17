#!/usr/bin/env python3
# Copyright (c) clast-project. All rights reserved.
# Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.
"""A reader-only `parquity.bridge.v1` engine backed by Spark's vectorized Parquet reader.

WHY SPARK AND WHY READER-ONLY. #269 was a defect in our default write path: every FLOAT and
DOUBLE column we wrote was unreadable by Spark. Parquity has a writer-by-reader matrix built for
exactly that class of bug and it did not catch it, because its five engines are all Python
distributions and Spark is not one. We do not need Spark as a writer -- we need it as the reader
that says whether what we wrote is reachable. `info.directions` is documented as a non-empty
subset of read and write, and `["read"]` alone is accepted.

THE ORACLE THIS EXISTS TO BE, measured here on Spark 4.0.1 / JDK 17 against pyarrow-written files
whose encodings were read back from the footer rather than assumed from the write option:

    reader                          V1+BSS   V2+BSS   V1+PLAIN
    vectorized (the default)        FAIL     FAIL     OK
    spark.sql.parquet.
      enableVectorizedReader=false  OK       OK       OK

So it is not a page-version split, and it is not Parquet: it is the vectorized path, which is what
every Spark user gets unless they have turned it off. That is the shape of consumer risk Parquity
has no verdict for -- a file four of five engines read happily and a major ecosystem reader cannot
-- and it is why this bridge leaves the reader on its default.

ONE SUBPROCESS PER OPERATION IS THE PROBLEM THIS SOLVES. Parquity launches the bridge afresh for
every read, and a JVM plus SparkSession costs 15-20 seconds, against a `timeout_seconds` bounded
at 300. A naive bridge would work and be useless at fuzz volumes. So the executable Parquity
launches is a thin CLIENT of a long-lived server holding one session, which it starts on first
use -- the same shape as `spark_driver.py`'s serve mode, over a loopback socket rather than stdin
because there is no parent process here to hold the pipe open.

    info                                   -- no session; answers from `pyspark.__version__`
    read --parquet <path> --arrow <path>   -- through the server
    stop                                   -- shuts the server down, for scripts and CI

EXIT CODES ARE THE CONTRACT, as in the EngineeredWood bridge beside this one: 0 succeeded, 1 is a
failure of the implementation under test and is recorded as evidence, 2 is a request this bridge
could not understand or serve and must stop the run instead of being filed as a Parquet defect.
A Spark read failure is 1 -- it is the finding. A missing pyspark is 2 -- it says nothing about
the file.
"""
from __future__ import annotations

import json
import os
import secrets
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import re

PROTOCOL = "parquity.bridge.v1"
ENGINE_NAME = "spark"

# Parquity's own limits, restated so that a detail is trimmed before it is sent rather than after.
MAX_KIND_LENGTH = 64
MAX_DETAIL_CHARS = 2000
_CONDITION_PATTERN = re.compile(r"\[([A-Z][A-Z0-9_.]{2,60})\]")

SUCCESS = 0
PROVIDER_FAILURE = 1
REQUEST_REJECTED = 2

# Long enough for a cold JVM on a loaded machine, and far short of Parquity's own ceiling.
SERVER_START_TIMEOUT = 180.0
REQUEST_TIMEOUT = 300.0

# The server goes away on its own so that a fuzz run does not leave a JVM behind for the day.
IDLE_SHUTDOWN_SECONDS = float(os.environ.get("EW_SPARK_BRIDGE_IDLE", "1800"))

# Spark and py4j print freely, and Parquity parses our stdout. Keep them apart.
_REAL_STDOUT = sys.stdout
sys.stdout = sys.stderr


def state_directory() -> Path:
    """Where the client and server rendezvous.

    Under the system temp directory by default so that nothing is left in the repository, and
    overridable so that two checkouts -- or two Spark versions -- do not share one server.
    """
    configured = os.environ.get("EW_SPARK_BRIDGE_STATE")
    root = Path(configured) if configured else Path(tempfile.gettempdir()) / "parquity-spark-bridge"
    root.mkdir(parents=True, exist_ok=True)
    return root


def endpoint_file() -> Path:
    return state_directory() / "endpoint.json"


# ---------------------------------------------------------------------------------------------
# The client: what Parquity launches.
# ---------------------------------------------------------------------------------------------


def main(argv: list[str]) -> int:
    if not argv:
        return reject("UsageError", "no operation was supplied")

    operation = argv[0]
    try:
        if operation == "info":
            return info()
        if operation == "read":
            return read(argv[1:])
        if operation == "stop":
            return stop()
        if operation == "serve":
            return serve()
        return reject("UsageError", f"unknown operation: {operation}")
    except BridgeRejection as rejection:
        return reject(rejection.kind, rejection.detail)


class BridgeRejection(Exception):
    """A request this bridge cannot serve, which must not be filed as a Parquet finding."""

    def __init__(self, kind: str, detail: str) -> None:
        super().__init__(detail)
        self.kind = kind
        self.detail = detail


def info() -> int:
    """Answers without starting a session, because Parquity probes this at configuration time."""
    try:
        import pyspark
    except ImportError as error:
        raise BridgeRejection("PySparkMissing", f"pyspark could not be imported: {error}") from error

    emit(
        {
            "protocol": PROTOCOL,
            "engine": ENGINE_NAME,
            "version": pyspark.__version__,
            # Reader only, deliberately. See the module docstring.
            "directions": ["read"],
        }
    )
    return SUCCESS


def read(arguments: list[str]) -> int:
    options = parse_arguments(arguments, {"--parquet", "--arrow"})
    parquet = options.get("--parquet")
    arrow = options.get("--arrow")
    if not parquet or not arrow:
        raise BridgeRejection("UsageError", "read requires --parquet and --arrow")

    response = call({"op": "read", "parquet": str(Path(parquet)), "arrow": str(Path(arrow))})

    if response.get("status") == "OK":
        emit({"status": "OK"})
        return SUCCESS

    # A read that failed IS the observation. Everything else is a bridge problem.
    kind = response.get("kind") or "SparkReadError"
    detail = response.get("detail") or "the Spark reader failed without a detail"
    if response.get("status") == "REJECTED":
        return reject(kind, detail)

    emit({"status": "ERROR", "kind": kind, "detail": detail})
    return PROVIDER_FAILURE


def stop() -> int:
    endpoint = load_endpoint()
    if endpoint is None:
        emit({"status": "OK"})
        return SUCCESS
    try:
        exchange(endpoint, {"op": "stop"})
    except OSError:
        pass
    endpoint_file().unlink(missing_ok=True)
    emit({"status": "OK"})
    return SUCCESS


def call(request: dict[str, object]) -> dict[str, object]:
    """Sends a request to the server, starting it if it is not already there."""
    endpoint = load_endpoint()
    if endpoint is not None:
        try:
            return exchange(endpoint, request)
        except OSError:
            # A stale endpoint from a server that has gone; fall through and start a new one.
            endpoint_file().unlink(missing_ok=True)

    endpoint = start_server()
    return exchange(endpoint, request)


def load_endpoint() -> tuple[int, str] | None:
    try:
        payload = json.loads(endpoint_file().read_text(encoding="utf-8"))
        return int(payload["port"]), str(payload["token"])
    except (OSError, ValueError, KeyError):
        return None


def start_server() -> tuple[int, str]:
    """Launches the server detached and waits for it to publish an endpoint."""
    before = load_endpoint()

    creation = 0
    if os.name == "nt":
        # DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP: it must outlive this client, and it must
        # not take a Ctrl-C aimed at the Parquity run with it.
        creation = 0x00000008 | 0x00000200

    subprocess.Popen(  # noqa: S603 - our own interpreter and script.
        [sys.executable, str(Path(__file__).resolve()), "serve"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=creation,
        close_fds=True,
    )

    deadline = time.monotonic() + SERVER_START_TIMEOUT
    while time.monotonic() < deadline:
        endpoint = load_endpoint()
        if endpoint is not None and endpoint != before:
            try:
                exchange(endpoint, {"op": "ping"})
                return endpoint
            except OSError:
                pass
        time.sleep(0.25)

    raise BridgeRejection(
        "SparkServerUnavailable",
        f"the Spark bridge server did not answer within {SERVER_START_TIMEOUT:.0f}s",
    )


def exchange(endpoint: tuple[int, str], request: dict[str, object]) -> dict[str, object]:
    port, token = endpoint
    with socket.create_connection(("127.0.0.1", port), timeout=REQUEST_TIMEOUT) as client:
        client.settimeout(REQUEST_TIMEOUT)
        payload = dict(request)
        payload["token"] = token
        client.sendall((json.dumps(payload) + "\n").encode("utf-8"))

        chunks: list[bytes] = []
        while not chunks or b"\n" not in chunks[-1]:
            chunk = client.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)

    body = b"".join(chunks).decode("utf-8", "replace").strip()
    if not body:
        raise OSError("the Spark bridge server closed without answering")
    return json.loads(body)


# ---------------------------------------------------------------------------------------------
# The server: one SparkSession, many requests.
# ---------------------------------------------------------------------------------------------


def serve() -> int:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(8)
    port = listener.getsockname()[1]
    token = secrets.token_hex(16)

    publish_endpoint(port, token)
    session = None

    try:
        while True:
            listener.settimeout(IDLE_SHUTDOWN_SECONDS)
            try:
                connection, _ = listener.accept()
            except socket.timeout:
                return SUCCESS

            with connection:
                request = receive(connection)
                if request is None or request.get("token") != token:
                    answer(connection, {"status": "REJECTED", "kind": "BadRequest",
                                        "detail": "malformed request or bad token"})
                    continue

                operation = request.get("op")
                if operation == "ping":
                    answer(connection, {"status": "OK"})
                elif operation == "stop":
                    answer(connection, {"status": "OK"})
                    return SUCCESS
                elif operation == "read":
                    session = session or open_session()
                    answer(connection, serve_read(session, request))
                else:
                    answer(connection, {"status": "REJECTED", "kind": "UsageError",
                                        "detail": f"unknown server operation: {operation!r}"})
    finally:
        endpoint_file().unlink(missing_ok=True)
        if session is not None:
            try:
                session.stop()
            except Exception:  # noqa: BLE001 - shutdown must not raise over a real result.
                pass


def publish_endpoint(port: int, token: str) -> None:
    """Writes the endpoint atomically, so a client never reads a half-written one."""
    target = endpoint_file()
    staging = target.with_suffix(".tmp")
    staging.write_text(json.dumps({"port": port, "token": token}), encoding="utf-8")
    os.replace(staging, target)


def open_session():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("parquity-spark-bridge")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        # NOT set, deliberately: spark.sql.parquet.enableVectorizedReader. The default is the
        # whole point -- it is the reader a Spark user actually gets, and it is the one that
        # cannot read BYTE_STREAM_SPLIT. Turning it off here would make the bridge agree with
        # everyone else and report nothing.
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    return session


def serve_read(session, request: dict[str, object]) -> dict[str, object]:
    import pyarrow as pa

    parquet = str(request.get("parquet", ""))
    arrow = str(request.get("arrow", ""))
    if not parquet or not arrow:
        return {"status": "REJECTED", "kind": "UsageError", "detail": "read needs parquet and arrow"}

    try:
        # toArrow() COLLECTS, which is what makes a decoding failure surface. A lazy plan would
        # report success for a file Spark cannot actually read.
        table = session.read.parquet(as_spark_path(parquet)).toArrow()
    except Exception as error:  # noqa: BLE001 - any read failure is the observation.
        return {"status": "ERROR", "kind": failure_kind(error), "detail": failure_detail(error)}

    try:
        with pa.OSFile(arrow, "wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
    except Exception as error:  # noqa: BLE001
        return {
            "status": "REJECTED",
            "kind": "ArrowWriteFailed",
            "detail": f"the table read back but could not be written as Arrow IPC: {error}",
        }

    return {"status": "OK"}


def as_spark_path(path: str) -> str:
    """Spark takes a URI-ish path; a Windows backslash in one is a path it will not find."""
    return str(Path(path).resolve()).replace("\\", "/")


def failure_kind(error: Exception) -> str:
    """A short, protocol-legal name for what failed, taken from the ROOT cause.

    Parquity requires `^[A-Za-z_][A-Za-z0-9_.]*$` and at most 64 characters. The outermost
    exception is always the same `SparkException: Exception thrown in awaitResult`, which names
    nothing; the innermost one is `SparkUnsupportedOperationException`, which names the defect.
    """
    root = root_cause(str(error))
    head = root.split(":", 1)[0].strip()
    simple = head.rsplit(".", 1)[-1]

    if simple and simple[0].isalpha() and simple.isidentifier() and len(simple) <= MAX_KIND_LENGTH:
        return simple

    return type(error).__name__[:MAX_KIND_LENGTH] or "SparkReadError"


def failure_detail(error: Exception) -> str:
    """The root cause and where it was raised, rather than sixteen frames of Py4J.

    WHAT THIS IS FOR. Parquity records the detail as evidence and caps it at 2 KB, and a Spark
    failure arrives as a stack that buries its own cause: the top of it is `awaitResult`, the
    middle is reflection and py4j, and the one line that says anything -- `Unsupported encoding:
    BYTE_STREAM_SPLIT`, raised in `VectorizedColumnReader` -- is at the bottom, past the cap. So
    the root cause leads, its first frame follows it because `VectorizedColumnReader` is the
    difference between "Spark cannot read this" and "Spark's DEFAULT reader cannot read this", and
    Spark's own error condition comes along when there is one.
    """
    text = str(error)
    root = root_cause(text) or " ".join(text.split())

    # The class name is already the kind, so the detail carries the message alone.
    message = root.split(": ", 1)[1] if ": " in root and root.split(":", 1)[0].count(".") else root

    frame = first_frame(text)
    if frame:
        message = f"{message} [raised at {frame}]"

    condition = spark_condition(text)
    if condition and condition not in message:
        message = f"{condition}: {message}"

    return message[:MAX_DETAIL_CHARS]


def root_cause(text: str) -> str:
    """The message of the innermost `Caused by`, with its stack trimmed off."""
    segments = text.split("Caused by:")
    return headline(segments[-1] if len(segments) > 1 else text)


def headline(segment: str) -> str:
    """One exception's message: everything before the stack frames begin."""
    flattened = " ".join(segment.split())
    cut = flattened.find(" at ")
    return (flattened[:cut] if cut > 0 else flattened).strip()


def first_frame(text: str) -> str:
    """The frame the root cause was raised in, as `Class.method`.

    It is what separates the vectorized reader from the fallback one, which is the whole question
    this bridge exists to ask.
    """
    # FLATTENED FIRST. A Java stack separates frames with a newline and a tab, not a space, so
    # searching the raw text for " at " finds nothing -- which is what it did until this was
    # measured against a real trace.
    segment = " ".join(text.split("Caused by:")[-1].split())
    marker = segment.find(" at ")
    if marker < 0:
        return ""

    qualified = segment[marker + 4 :].split("(", 1)[0]
    return ".".join(qualified.rsplit(".", 2)[-2:])


def spark_condition(text: str) -> str:
    """Spark's own name for the error, e.g. `FAILED_READ_FILE.NO_HINT`, when it states one."""
    match = _CONDITION_PATTERN.search(text)
    return match.group(1) if match else ""


# ---------------------------------------------------------------------------------------------
# Plumbing.
# ---------------------------------------------------------------------------------------------


def receive(connection: socket.socket) -> dict[str, object] | None:
    connection.settimeout(REQUEST_TIMEOUT)
    chunks: list[bytes] = []
    while not chunks or b"\n" not in chunks[-1]:
        try:
            chunk = connection.recv(65536)
        except OSError:
            return None
        if not chunk:
            break
        chunks.append(chunk)
    try:
        return json.loads(b"".join(chunks).decode("utf-8"))
    except ValueError:
        return None


def answer(connection: socket.socket, payload: dict[str, object]) -> None:
    try:
        connection.sendall((json.dumps(payload) + "\n").encode("utf-8"))
    except OSError:
        pass


def parse_arguments(arguments: list[str], allowed: set[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    index = 0
    while index < len(arguments):
        name = arguments[index]
        if name not in allowed:
            raise BridgeRejection("UsageError", f"unexpected argument: {name}")
        if index + 1 >= len(arguments):
            raise BridgeRejection("UsageError", f"{name} needs a value")
        parsed[name] = arguments[index + 1]
        index += 2
    return parsed


def emit(payload: dict[str, object]) -> None:
    _REAL_STDOUT.write(json.dumps(payload))
    _REAL_STDOUT.write("\n")
    _REAL_STDOUT.flush()


def reject(kind: str, detail: str) -> int:
    emit({"status": "ERROR", "kind": kind, "detail": detail})
    return REQUEST_REJECTED


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
