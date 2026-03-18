import os
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
    body = await request.body()

    # Копируем заголовки, но удаляем 'host', чтобы httpx подставил правильный хост целевого сервиса
    headers = dict(request.headers)
    headers.pop("host", None)

    proxy_req = client.build_request(
        method=request.method,
        url=url,
        headers=headers,
        content=body,
        params=request.query_params,
    )

    proxy_resp = await client.send(proxy_req, stream=False)

    return Response(
        content=proxy_resp.content,
        status_code=proxy_resp.status_code,
        headers=dict(proxy_resp.headers)
    )


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