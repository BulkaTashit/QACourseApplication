import asyncio
import json
from aiokafka import AIOKafkaConsumer
from clickhouse_connect import get_client
from fastapi import FastAPI

app = FastAPI()

# Конфигурация Kafka и ClickHouse
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
ORDER_TOPIC = "order_topic"

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_DB = "default"
CLICKHOUSE_TABLE = "orders"

# Подключение к ClickHouse
client = get_client(host=CLICKHOUSE_HOST, database=CLICKHOUSE_DB)

# Создаем таблицу, если её нет
client.command(f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_TABLE} (
    order_number String,
    order_date DateTime,
    order_data String
) ENGINE = MergeTree()
ORDER BY order_date
""")


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

            # Вставляем данные в ClickHouse
            client.insert(
                CLICKHOUSE_TABLE,
                [[order_data["order_number"], order_data["order_date"], json.dumps(order_data)]],
                column_names=["order_number", "order_date", "order_data"]
            )

    finally:
        await consumer.stop()


@app.get("/order_count/")
async def get_order_count():
    """ Возвращает количество заказов за сегодня """
    result = client.query(
        f"SELECT COUNT(*) FROM {CLICKHOUSE_TABLE} WHERE order_date >= today()"
    ).result_rows
    return {"order_count_today": result[0][0]}


@app.get("/latest_orders/")
async def get_latest_orders():
    """ Возвращает 5 последних заказов """
    result = client.query(
        f"SELECT order_number, order_date FROM {CLICKHOUSE_TABLE} ORDER BY order_date DESC LIMIT 5"
    ).result_rows
    return {"latest_orders": result}


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(consume_orders())

    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)
