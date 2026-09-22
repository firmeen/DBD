from __future__ import annotations

import asyncio
import hmac
import logging
import os
import re
import secrets
import shutil
import sys
import time
import uuid
from http import HTTPStatus

from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed


BRIDGE_HOST = "127.0.0.1"
BRIDGE_PORT = int(os.getenv("DBD_BRIDGE_PORT", "8787"))
MYSQL_HOST = os.getenv("DBD_MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("DBD_MYSQL_PORT", "3307"))
MAX_SESSIONS = int(os.getenv("DBD_BRIDGE_MAX_SESSIONS", "32"))
MYSQL_CONNECT_TIMEOUT = float(os.getenv("DBD_MYSQL_CONNECT_TIMEOUT", "5"))
WS_PATH = "/dbd"
HEALTH_PATH = "/healthz"
CHUNK_SIZE = 64 * 1024
WS_MAX_SIZE = 1024 * 1024
WS_MAX_QUEUE = 32
WS_WRITE_LIMIT = 256 * 1024
PING_INTERVAL = 20
PING_TIMEOUT = 20
CLOSE_TIMEOUT = 5
CLOUDFLARED_START_TIMEOUT = 60
TRY_CLOUDFLARE_RE = re.compile(r"https://[a-zA-Z0-9-]+\.trycloudflare\.com")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dbd.gateway")

session_limit = asyncio.Semaphore(MAX_SESSIONS)


def resolve_gateway_token() -> tuple[str, bool]:
    token = os.getenv("DBD_GATEWAY_TOKEN")
    if token:
        if len(token) < 32:
            raise RuntimeError("DBD_GATEWAY_TOKEN is too short; use at least 32 characters.")
        return token, False
    return secrets.token_hex(32), True


async def mysql_ready() -> bool:
    try:
        _reader, writer = await asyncio.wait_for(
            asyncio.open_connection(MYSQL_HOST, MYSQL_PORT),
            timeout=3,
        )
        writer.close()
        await writer.wait_closed()
        return True
    except (OSError, asyncio.TimeoutError):
        return False


async def wait_for_mysql() -> None:
    log.info("Checking MySQL %s:%d...", MYSQL_HOST, MYSQL_PORT)
    while True:
        if await mysql_ready():
            log.info("MySQL READY %s:%d", MYSQL_HOST, MYSQL_PORT)
            return
        log.warning("MySQL not ready. Retrying...")
        await asyncio.sleep(3)


def make_process_request(gateway_token: str):
    async def process_request(connection: ServerConnection, request):
        path = request.path.split("?", 1)[0]
        if path == HEALTH_PATH:
            return connection.respond(HTTPStatus.OK, "DBD MYSQL BRIDGE OK\n")
        if path != WS_PATH:
            return connection.respond(HTTPStatus.NOT_FOUND, "Not Found\n")

        supplied_token = request.headers.get("X-DBD-Gateway-Token", "")
        if not hmac.compare_digest(supplied_token, gateway_token):
            log.warning("Unauthorized gateway request rejected")
            return connection.respond(HTTPStatus.UNAUTHORIZED, "Unauthorized\n")
        return None

    return process_request


async def bridge_session(websocket: ServerConnection) -> None:
    session_id = uuid.uuid4().hex[:10]
    mysql_writer = None
    acquired = False
    client_to_mysql = 0
    mysql_to_client = 0

    try:
        try:
            await asyncio.wait_for(session_limit.acquire(), timeout=2)
            acquired = True
        except asyncio.TimeoutError:
            await websocket.close(code=1013, reason="Gateway busy")
            return

        headers = websocket.request.headers
        client_name = headers.get("X-DBD-Client", "unknown")
        cf_ip = headers.get("CF-Connecting-IP", "unknown")
        cf_ray = headers.get("CF-Ray", "unknown")
        log.info(
            "[%s] OPEN client=%s cf_ip=%s ray=%s",
            session_id,
            client_name,
            cf_ip,
            cf_ray,
        )

        try:
            mysql_reader, mysql_writer = await asyncio.wait_for(
                asyncio.open_connection(MYSQL_HOST, MYSQL_PORT),
                timeout=MYSQL_CONNECT_TIMEOUT,
            )
        except Exception as exc:
            log.error("[%s] MYSQL CONNECT FAILED: %s", session_id, exc)
            await websocket.close(code=1011, reason="MySQL unavailable")
            return

        log.info("[%s] MYSQL CONNECTED %s:%d", session_id, MYSQL_HOST, MYSQL_PORT)

        async def ws_to_mysql() -> None:
            nonlocal client_to_mysql
            try:
                async for message in websocket:
                    if not isinstance(message, (bytes, bytearray, memoryview)):
                        raise RuntimeError("Text WebSocket frames aren't allowed")
                    data = bytes(message)
                    mysql_writer.write(data)
                    await mysql_writer.drain()
                    client_to_mysql += len(data)
            except ConnectionClosed:
                return

        async def mysql_to_ws() -> None:
            nonlocal mysql_to_client
            while True:
                data = await mysql_reader.read(CHUNK_SIZE)
                if not data:
                    return
                await websocket.send(data)
                mysql_to_client += len(data)

        task_ws_mysql = asyncio.create_task(ws_to_mysql())
        task_mysql_ws = asyncio.create_task(mysql_to_ws())
        done, pending = await asyncio.wait(
            {task_ws_mysql, task_mysql_ws},
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
    except ConnectionClosed:
        pass
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("[%s] SESSION ERROR", session_id)
    finally:
        if mysql_writer is not None:
            mysql_writer.close()
            try:
                await mysql_writer.wait_closed()
            except Exception:
                pass
        if acquired:
            session_limit.release()
        log.info(
            "[%s] CLOSED client_to_mysql=%d mysql_to_client=%d",
            session_id,
            client_to_mysql,
            mysql_to_client,
        )


async def stream_cloudflared_output(process: asyncio.subprocess.Process) -> str:
    if process.stdout is None:
        raise RuntimeError("Unable to capture cloudflared output")

    started = time.monotonic()
    public_url = None
    while True:
        if time.monotonic() - started > CLOUDFLARED_START_TIMEOUT and public_url is None:
            raise RuntimeError("Timed out waiting for Quick Tunnel URL.")
        try:
            raw_line = await asyncio.wait_for(process.stdout.readline(), timeout=1)
        except asyncio.TimeoutError:
            if process.returncode is not None:
                raise RuntimeError("cloudflared exited before tunnel became ready")
            continue

        if not raw_line:
            return_code = await process.wait()
            raise RuntimeError(f"cloudflared exited with code {return_code}")

        line = raw_line.decode("utf-8", errors="replace").rstrip()
        if line:
            log.info("[cloudflared] %s", line)

        if public_url is None:
            match = TRY_CLOUDFLARE_RE.search(line)
            if match:
                public_url = match.group(0)
                return public_url


def print_ready(https_url: str, gateway_token: str) -> None:
    ws_url = https_url.replace("https://", "wss://", 1) + WS_PATH
    print("\n" + "=" * 72)
    print(" DBD PRIVATE MYSQL GATEWAY - READY")
    print("=" * 72 + "\n")
    print(f" MySQL       : {MYSQL_HOST}:{MYSQL_PORT}")
    print(f" Bridge      : {BRIDGE_HOST}:{BRIDGE_PORT}")
    print(f" HTTPS       : {https_url}")
    print(f" CLIENT WSS  : {ws_url}")
    print(f" TOKEN       : {gateway_token}")
    print("\n CLIENT:\n")
    print(f' $env:DBD_WS_URL = "{ws_url}"')
    print(f' $env:DBD_GATEWAY_TOKEN = "{gateway_token}"')
    print(' $env:DBD_CLIENT_NAME = "tipdi-client-01"')
    print(" python .\\dbd_client.py")
    print("\n" + "=" * 72 + "\n")


async def start_quick_tunnel(gateway_token: str) -> asyncio.subprocess.Process:
    cloudflared_exe = shutil.which("cloudflared")
    if not cloudflared_exe:
        raise RuntimeError("cloudflared.exe not found in PATH")

    origin_url = f"http://{BRIDGE_HOST}:{BRIDGE_PORT}"
    log.info("Starting Cloudflare Quick Tunnel...")
    log.info("Origin: %s", origin_url)

    process = await asyncio.create_subprocess_exec(
        cloudflared_exe,
        "tunnel",
        "--url",
        origin_url,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )

    public_url = await stream_cloudflared_output(process)
    print_ready(public_url, gateway_token)

    async def drain_output() -> None:
        if process.stdout is None:
            return
        async for raw_line in process.stdout:
            line = raw_line.decode("utf-8", errors="replace").rstrip()
            if line:
                log.info("[cloudflared] %s", line)

    asyncio.create_task(drain_output())
    return process


async def stop_cloudflared(process: asyncio.subprocess.Process | None) -> None:
    if process is None or process.returncode is not None:
        return
    log.info("Stopping cloudflared...")
    try:
        process.terminate()
        await asyncio.wait_for(process.wait(), timeout=8)
    except asyncio.TimeoutError:
        log.warning("cloudflared didn't stop gracefully. Killing...")
        process.kill()
        await process.wait()


async def main() -> None:
    print("\n" + "=" * 72)
    print(" DBD PRIVATE MYSQL GATEWAY")
    print(" Python Bridge + Cloudflare Quick Tunnel")
    print("=" * 72 + "\n")

    gateway_token, generated = resolve_gateway_token()
    if generated:
        log.info("No DBD_GATEWAY_TOKEN supplied.")
        log.info("Generated a new 256-bit gateway token.")
    else:
        log.info("Using DBD_GATEWAY_TOKEN from environment.")

    await wait_for_mysql()
    cloudflared_process = None

    async with serve(
        bridge_session,
        BRIDGE_HOST,
        BRIDGE_PORT,
        process_request=make_process_request(gateway_token),
        compression=None,
        ping_interval=PING_INTERVAL,
        ping_timeout=PING_TIMEOUT,
        close_timeout=CLOSE_TIMEOUT,
        max_size=WS_MAX_SIZE,
        max_queue=WS_MAX_QUEUE,
        write_limit=WS_WRITE_LIMIT,
        server_header=None,
    ):
        log.info("Bridge READY http://%s:%d%s", BRIDGE_HOST, BRIDGE_PORT, WS_PATH)
        log.info("Health READY http://%s:%d%s", BRIDGE_HOST, BRIDGE_PORT, HEALTH_PATH)
        try:
            cloudflared_process = await start_quick_tunnel(gateway_token)
            await cloudflared_process.wait()
            raise RuntimeError(
                f"cloudflared exited unexpectedly with code {cloudflared_process.returncode}"
            )
        finally:
            await stop_cloudflared(cloudflared_process)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nDBD gateway stopped.")
    except Exception as exc:
        print("\n" + "=" * 72)
        print(" DBD GATEWAY FAILED")
        print("=" * 72 + "\n")
        print(exc)
        print()
        sys.exit(1)
