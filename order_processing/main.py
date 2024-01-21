import asyncio
import json
import os
import logging

import aioredis

from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer

app = FastAPI()

# Конфигурация Kafka
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
ORDER_TOPIC = os.environ.get("ORDER_TOPIC", "order_topic")

# Конфигурация Redis
REDIS_HOST = os.environ.get("REDIS_HOST", "redis-container")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))


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
            await redis.hset(redis_key, "quantity", quantity)
            await redis.hset(redis_key, "price", price)
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON: {e}")


async def consume(redis: aioredis.Redis):
    consumer = AIOKafkaConsumer(
        ORDER_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await consumer.start()
    try:
        print("Kafka Consumer started successfully.")
        async for msg in consumer:
            try:
                order_data = msg.value
                logging.info(f"Received message: {order_data}")
                await process_order_message(order_data, redis)
            except Exception as e:
                logging.error(f"Error processing message: {e}")
    finally:
        await consumer.stop()


@app.on_event("startup")
async def startup():
    redis = await aioredis.create_redis_pool((REDIS_HOST, REDIS_PORT))

    await asyncio.sleep(20)

    asyncio.create_task(consume(redis))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8004)
