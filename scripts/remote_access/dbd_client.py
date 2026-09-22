from __future__ import annotations

import asyncio
import logging
import os
import sys
import urllib.error
import urllib.request
import uuid
from urllib.parse import urlsplit

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

LOCAL_HOST = "127.0.0.1"
LOCAL_PORT = int(os.getenv("DBD_CLIENT_LOCAL_PORT", "13307"))
CHUNK_SIZE = 64 * 1024

PREFLIGHT_TIMEOUT = float(os.getenv("DBD_PREFLIGHT_TIMEOUT", "8"))
WS_OPEN_TIMEOUT = float(os.getenv("DBD_WS_OPEN_TIMEOUT", "10"))
PING_INTERVAL = 20
PING_TIMEOUT = 20
CLOSE_TIMEOUT = 5

WS_MAX_SIZE = 1024 * 1024
WS_MAX_QUEUE = 32
WS_WRITE_LIMIT = 256 * 1024

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dbd.client")


def get_health_url(ws_url: str) -> str:
    parsed = urlsplit(ws_url)
    if parsed.scheme != "wss":
        raise RuntimeError("DBD_WS_URL must start with wss://")
    if not parsed.netloc:
        raise RuntimeError("DBD_WS_URL has no hostname.")
    return f"https://{parsed.netloc}/healthz"


def preflight_gateway(ws_url: str, timeout: float = PREFLIGHT_TIMEOUT) -> None:
    health_url = get_health_url(ws_url)
    print("\n[PRECHECK] Testing remote gateway...")
    print(f"[PRECHECK] {health_url}")

    request = urllib.request.Request(
        health_url,
        method="GET",
        headers={"User-Agent": "DBD-Private-Client/1.0"},
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = response.status
            body = response.read(2048).decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        try:
            response_body = exc.read(2048).decode("utf-8", errors="replace")
        except Exception:
            response_body = ""
        raise RuntimeError(
            "\nRemote DBD gateway was rejected.\n\n"
            f"HTTP status : {exc.code}\n"
            f"URL         : {health_url}\n"
            f"Response    : {response_body[:1000]}\n"
        ) from exc
    except urllib.error.URLError as exc:
        reason = getattr(exc, "reason", exc)
        raise RuntimeError(
            "\nCannot reach DBD remote gateway.\n\n"
            f"URL    : {health_url}\n"
            f"Reason : {reason}\n"
        ) from exc
    except TimeoutError as exc:
        raise RuntimeError(
            "\nDBD gateway preflight timed out.\n\n"
            f"URL     : {health_url}\n"
            f"Timeout : {timeout}s\n"
        ) from exc
    except Exception as exc:
        raise RuntimeError(
            "\nDBD gateway preflight failed.\n\n"
            f"URL    : {health_url}\n"
            f"Reason : {exc}\n"
        ) from exc

    if status != 200:
        raise RuntimeError(
            "\nUnexpected DBD gateway status.\n\n"
            f"HTTP status : {status}\n"
            f"URL         : {health_url}\n"
        )
    if "DBD MYSQL BRIDGE OK" not in body:
        raise RuntimeError(
            "\nRemote endpoint responded, but it is not the expected DBD gateway.\n\n"
            f"URL      : {health_url}\n"
            f"Response : {body[:1000]}\n"
        )

    print("[PRECHECK] Remote gateway: PASS")


async def handle_local_mysql(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    ws_url: str,
    gateway_token: str,
    client_name: str,
) -> None:
    session_id = uuid.uuid4().hex[:10]
    peer = writer.get_extra_info("peername")
    local_to_remote = 0
    remote_to_local = 0

    log.info("[%s] LOCAL OPEN peer=%s", session_id, peer)

    headers = {
        "X-DBD-Gateway-Token": gateway_token,
        "X-DBD-Client": client_name,
    }

    try:
        async with connect(
            ws_url,
            additional_headers=headers,
            compression=None,
            open_timeout=WS_OPEN_TIMEOUT,
            ping_interval=PING_INTERVAL,
            ping_timeout=PING_TIMEOUT,
            close_timeout=CLOSE_TIMEOUT,
            max_size=WS_MAX_SIZE,
            max_queue=WS_MAX_QUEUE,
            write_limit=WS_WRITE_LIMIT,
        ) as websocket:
            log.info("[%s] REMOTE CONNECTED", session_id)

            async def local_to_ws() -> None:
                nonlocal local_to_remote
                while True:
                    data = await reader.read(CHUNK_SIZE)
                    if not data:
                        return
                    await websocket.send(data)
                    local_to_remote += len(data)

            async def ws_to_local() -> None:
                nonlocal remote_to_local
                try:
                    async for message in websocket:
                        if not isinstance(message, (bytes, bytearray, memoryview)):
                            raise RuntimeError("Unexpected text WebSocket frame.")
                        data = bytes(message)
                        writer.write(data)
                        await writer.drain()
                        remote_to_local += len(data)
                except ConnectionClosed:
                    return

            send_task = asyncio.create_task(local_to_ws(), name=f"{session_id}-local-to-ws")
            receive_task = asyncio.create_task(ws_to_local(), name=f"{session_id}-ws-to-local")
            done, pending = await asyncio.wait(
                {send_task, receive_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in done:
                if task.cancelled():
                    continue
                exc = task.exception()
                if exc is not None:
                    raise exc

            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
    except asyncio.CancelledError:
        raise
    except TimeoutError:
        log.error("[%s] CONNECTION FAILED: remote WebSocket timeout", session_id)
    except Exception as exc:
        log.error("[%s] CONNECTION FAILED: %s", session_id, exc)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        log.info("[%s] CLOSED tx=%d rx=%d", session_id, local_to_remote, remote_to_local)


async def run_client(ws_url: str, gateway_token: str, client_name: str) -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await handle_local_mysql(
            reader,
            writer,
            ws_url=ws_url,
            gateway_token=gateway_token,
            client_name=client_name,
        )

    server = await asyncio.start_server(handler, LOCAL_HOST, LOCAL_PORT)

    print("\n" + "=" * 72)
    print(" DBD PRIVATE MYSQL CLIENT")
    print("=" * 72 + "\n")
    print(f" Client     : {client_name}")
    print(f" Remote WSS : {ws_url}")
    print(f" Local      : {LOCAL_HOST}:{LOCAL_PORT}")
    print("\n DBeaver / PyMySQL / VS Code")
    print(" Host       : 127.0.0.1")
    print(f" Port       : {LOCAL_PORT}")
    print(" Database   : dbd")
    print("\n" + "=" * 72 + "\n")

    async with server:
        await server.serve_forever()


async def main() -> None:
    ws_url = os.environ["DBD_WS_URL"]
    gateway_token = os.environ["DBD_GATEWAY_TOKEN"]
    client_name = os.getenv("DBD_CLIENT_NAME", "dbd-client")

    if not ws_url.startswith("wss://"):
        raise RuntimeError("DBD_WS_URL must start with wss://")
    if not gateway_token:
        raise RuntimeError("DBD_GATEWAY_TOKEN is empty.")

    await asyncio.to_thread(preflight_gateway, ws_url)
    await run_client(ws_url, gateway_token, client_name)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nDBD client stopped.")
        sys.exit(0)
    except Exception as exc:
        print("\n" + "=" * 72)
        print(" DBD CLIENT FAILED")
        print("=" * 72 + "\n")
        print(exc)
        print()
        sys.exit(1)
