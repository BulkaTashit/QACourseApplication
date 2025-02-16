import asyncio
import json
import os
import logging
import redis.asyncio as aioredis

from contextlib import asynccontextmanager
from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация Kafka
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
ORDER_TOPIC = os.environ.get("ORDER_TOPIC", "order_topic")

# Конфигурация Redis
REDIS_HOST = os.environ.get("REDIS_HOST", "redis-container")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))


# Управление жизненным циклом приложения
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Подключение к Redis
    redis = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    await asyncio.sleep(20)  # Ожидание готовности Kafka
    asyncio.create_task(consume(redis))  # Запуск потребителя Kafka
    yield
    await redis.close()  # Закрытие соединения с Redis при завершении

app = FastAPI(lifespan=lifespan)


# Корневой маршрут
@app.get("/")
async def read_root():
    return {"message": "Добро пожаловать в FastAPI Kafka Consumer!"}


# Обработка сообщений из Kafka
async def process_order_message(order_data: bytes, redis: aioredis.Redis):
    try:
        # Декодируем байты в строку
        order_data_str = order_data.decode("utf-8")

        # Преобразуем строку JSON в словарь
        order_data_dict = json.loads(order_data_str)

        # Извлекаем информацию о заказе
        order_number = order_data_dict.get("order_number")
        quantity = order_data_dict.get("quantity")
        price = order_data_dict.get("price")

        if order_number and quantity and price:
            # Записываем информацию о заказе в Redis
            redis_key = f"order:{order_number}"
            await redis.hset(redis_key, mapping={"quantity": quantity, "price": price})
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка декодирования JSON: {e}")
    except Exception as e:
        logger.error(f"Ошибка обработки сообщения: {e}")


# Потребитель Kafka
async def consume(redis: aioredis.Redis):
    consumer = AIOKafkaConsumer(
        ORDER_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id="my-consumer-group")
    await consumer.start()
    try:
        logger.info("Kafka Consumer успешно запущен.")
        async for msg in consumer:
            try:
                order_data = msg.value
                logger.info(f"Получено сообщение: {order_data}")
                await process_order_message(order_data, redis)
            except Exception as e:
                logger.error(f"Ошибка обработки сообщения: {e}")
    finally:
        await consumer.stop()

# Запуск приложения
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8004)