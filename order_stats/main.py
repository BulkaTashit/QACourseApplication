import asyncio
import json
import os

from datetime import datetime
from aiokafka import AIOKafkaConsumer
from clickhouse_connect import get_client
from fastapi import FastAPI

app = FastAPI()

# Конфигурация Kafka и ClickHouse
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
ORDER_TOPIC = os.environ.get("ORDER_TOPIC", "order_topic")

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_USERNAME = "default"
CLICKHOUSE_PASSWORD = "your_secure_password"
CLICKHOUSE_DB = "mydatabase"
CLICKHOUSE_TABLE = "orders"

client = None  # ClickHouse клиент будет инициализирован в lifespan


async def consume_orders():
    """ Функция-потребитель Kafka """
    consumer = AIOKafkaConsumer(
        ORDER_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest"
    )
    await consumer.start()

    try:
        async for msg in consumer:
            order_data = json.loads(msg.value.decode("utf-8"))
            print(f"Получен заказ: {order_data}")

            # Конвертируем строку в datetime
            order_date = datetime.fromisoformat(order_data["order_date"])

            await asyncio.to_thread(
                client.insert,
                CLICKHOUSE_TABLE,
                [[order_data["order_number"], order_date, json.dumps(order_data)]],
                column_names=["order_number", "order_date", "order_data"]
            )

    finally:
        await consumer.stop()


@app.get("/order_count/")
async def get_order_count():
    """ Возвращает количество заказов за сегодня """
    result = await asyncio.to_thread(
        client.query, f"SELECT COUNT(*) FROM {CLICKHOUSE_TABLE} WHERE order_date >= today()"
    )
    return {"order_count_today": result.result_rows[0][0]}


@app.get("/latest_orders/")
async def get_latest_orders():
    """ Возвращает 5 последних заказов """
    result = await asyncio.to_thread(
        client.query, f"SELECT order_number, order_date FROM {CLICKHOUSE_TABLE} ORDER BY order_date DESC LIMIT 5"
    )
    return {"latest_orders": result.result_rows}


@app.on_event("startup")
async def startup_event():
    """ Инициализация ClickHouse клиента и запуск Kafka consumer """
    global client
    client = get_client(
        host=CLICKHOUSE_HOST, username=CLICKHOUSE_USERNAME, password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
    )

    # Создаем таблицу, если её нет
    await asyncio.to_thread(client.command, f"""
    CREATE TABLE IF NOT EXISTS {CLICKHOUSE_TABLE} (
        order_number String,
        order_date DateTime,
        order_data String
    ) ENGINE = MergeTree()
    ORDER BY order_date
    """)

    # Запускаем Kafka consumer
    asyncio.create_task(consume_orders())


@app.on_event("shutdown")
async def shutdown_event():
    """ Закрытие соединения с ClickHouse при выключении сервиса """
    global client
    if client:
        client.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005)
