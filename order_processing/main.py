import asyncio
import json
import os
import logging
import redis.asyncio as aioredis

from contextlib import asynccontextmanager
from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация Kafka
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
ORDER_TOPIC = os.environ.get("ORDER_TOPIC", "order_topic")

# Конфигурация Redis
REDIS_HOST = os.environ.get("REDIS_HOST", "redis-container")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize Redis connection
    redis = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    await asyncio.sleep(20)  # Wait for Kafka to be ready
    asyncio.create_task(consume(redis))
    yield
    # Clean up
    await redis.close()

app = FastAPI(lifespan=lifespan)


@app.get("/")
async def read_root():
    return {"message": "Welcome to the FastAPI Kafka Consumer"}


async def process_order_message(order_data: bytes, redis: aioredis.Redis):
    try:
        # Декодировать байты в строку
        order_data_str = order_data.decode("utf-8")

        # Преобразовать строку JSON в словарь
        order_data_dict = json.loads(order_data_str)

        # Извлечь информацию о заказе
        order_number = order_data_dict.get("order_number")
        quantity = order_data_dict.get("quantity")
        price = order_data_dict.get("price")

        if order_number and quantity and price:
            # Записать информацию о заказе в Redis
            redis_key = f"order:{order_number}"
            await redis.hset(redis_key, mapping={"quantity": quantity, "price": price})
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON: {e}")
    except Exception as e:
        logger.error(f"Error processing order message: {e}")


async def consume(redis: aioredis.Redis):
    consumer = AIOKafkaConsumer(
        ORDER_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id="my-consumer-group")
    await consumer.start()
    try:
        logger.info("Kafka Consumer started successfully.")
        async for msg in consumer:
            try:
                order_data = msg.value
                logger.info(f"Received message: {order_data}")
                await process_order_message(order_data, redis)
            except Exception as e:
                logger.error(f"Error processing message: {e}")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8004)