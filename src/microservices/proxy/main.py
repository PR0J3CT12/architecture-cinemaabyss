import os
import json
import random
from fastapi import FastAPI, Request, Response
import httpx

app = FastAPI()

MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://events-service:8082")

GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "true").lower() == "true"
MOVIES_MIGRATION_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", "50"))

client = httpx.AsyncClient()


async def forward_request(url: str, request: Request) -> Response:
    """Функция для пересылки входящего запроса на целевой сервис."""
    body = await request.body()

    headers = dict(request.headers)
    headers.pop("host", None)
    headers.pop("content-length", None)

    content = body if body else None

    try:
        proxy_req = client.build_request(
            method=request.method,
            url=url,
            headers=headers,
            content=content,
            params=request.query_params,
        )

        proxy_resp = await client.send(proxy_req, stream=False)

        resp_headers = dict(proxy_resp.headers)
        resp_headers.pop("content-length", None)
        resp_headers.pop("content-encoding", None)
        resp_headers.pop("transfer-encoding", None)

        return Response(
            content=proxy_resp.content,
            status_code=proxy_resp.status_code,
            headers=resp_headers
        )
    except Exception as e:
        print(f"Ошибка проксирования на {url}: {e}")
        return Response(content=json.dumps({"error": f"Bad Gateway: {str(e)}"}), status_code=502)


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy_gateway(request: Request, path: str):
    """Единая точка входа, маршрутизирующая запросы по сервисам."""

    target_url = f"{MONOLITH_URL}/{path}"

    if path.startswith("api/events"):
        target_url = f"{EVENTS_SERVICE_URL}/{path}"

    elif path.startswith("api/movies"):
        if GRADUAL_MIGRATION:
            roll = random.randint(1, 100)
            if roll <= MOVIES_MIGRATION_PERCENT:
                target_url = f"{MOVIES_SERVICE_URL}/{path}"
        else:
            target_url = f"{MOVIES_SERVICE_URL}/{path}"

    print(f"Proxying {request.method} /{path} ---> {target_url}")
    return await forward_request(target_url, request)