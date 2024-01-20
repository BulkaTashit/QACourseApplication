import asyncio
import json
import uuid
from fastapi import FastAPI
from aiokafka import AIOKafkaProducer

app = FastAPI()

# Конфигурация Kafka
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
ORDER_TOPIC = "order_topic"


async def send_order_message(order_data: dict):
    try:
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        await producer.start()

        # Produce a single message
        res = await producer.send_and_wait(ORDER_TOPIC, json.dumps(order_data).encode("utf-8"))
        print(res)

    finally:
        await producer.stop()


@app.post("/place_order/")
async def place_order(order_data: dict = None):
    # Генерируем уникальный UUID для заказа
    order_uuid = str(uuid.uuid4())

    # Добавляем UUID к информации о заказе
    order_data["order_number"] = order_uuid

    # Отправляем заказ в Kafka
    await send_order_message(order_data)

    response_message = f"Заказ успешно размещен. Номер заказа: {order_uuid}"
    return {"message": response_message}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8003)
