import os
import json
import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "cinema_events")

producer: AIOKafkaProducer = None
consumer_task: asyncio.Task = None


async def consume_events():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id="events-consumer-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest"
    )
    # Пытаемся подключиться с ретраями, так как Kafka может стартовать не сразу
    while True:
        try:
            await consumer.start()
            break
        except Exception as e:
            logger.warning(f"Ожидание Kafka Consumer... {e}")
            await asyncio.sleep(3)

    try:
        async for msg in consumer:
            logger.info(f"[Consumer] Прочитано событие: {msg.value}")
    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer, consumer_task

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    # Ретраи для продюсера
    while True:
        try:
            await producer.start()
            logger.info("Kafka Producer запущен")
            break
        except Exception as e:
            logger.warning(f"Ожидание Kafka Producer... {e}")
            await asyncio.sleep(3)

    consumer_task = asyncio.create_task(consume_events())
    yield
    await producer.stop()
    consumer_task.cancel()


app = FastAPI(lifespan=lifespan)


# --- ЭНДПОИНТЫ ДЛЯ ТЕСТОВ ---

@app.get("/api/events/health")
async def health_check():
    """Тест: Health Check"""
    # Тест ожидает статус 200 и Status is true
    return {"status": True}


async def send_event(event_type: str, request: Request):
    """Вспомогательная функция для отправки событий"""
    try:
        data = await request.json()
    except:
        data = {}

    payload = {"event_type": event_type, "data": data}
    await producer.send_and_wait(KAFKA_TOPIC, payload)
    logger.info(f"Отправлено событие {event_type}")

    # Тест ожидает статус 201 (Created) и Response has status success
    from fastapi.responses import JSONResponse
    return JSONResponse(status_code=201, content={"status": "success"})


@app.post("/api/events/movie")
async def create_movie_event(request: Request):
    return await send_event("Movie", request)


@app.post("/api/events/user")
async def create_user_event(request: Request):
    return await send_event("User", request)


@app.post("/api/events/payment")
async def create_payment_event(request: Request):
    return await send_event("Payment", request)