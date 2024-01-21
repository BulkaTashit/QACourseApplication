import aio_pika
import asyncio
import json

from fastapi import FastAPI, Depends
from models import Payment
from database import get_database
from rabbitmq import get_rabbit_channel

app = FastAPI()

# Параметры подключения к RabbitMQ
RABBITMQ_HOST = 'rabbitmq-container'
RABBITMQ_PORT = 5672
RABBITMQ_QUEUE = 'inventory_queue'
RABBITMQ_USER = 'guest'
RABBITMQ_PASSWORD = 'guest'


@app.on_event("startup")
async def startup():
    db = await get_database()
    await db.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id serial PRIMARY KEY,
                amount numeric,
                description text,
                item_id integer
            )
        """)

    connection = None
    while connection is None:
        try:
            connection = await aio_pika.connect(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                login=RABBITMQ_USER,
                password=RABBITMQ_PASSWORD,
            )
            print("Connection to RabbitMQ successful.")
        except Exception as e:
            print(f"Waiting for RabbitMQ to start: {e}")
            await asyncio.sleep(1)


@app.post("/payments/")
async def create_payment(payment: Payment, db=Depends(get_database), channel=Depends(get_rabbit_channel)):
    async with db.transaction():
        # Ваш код для сохранения платежа в базе данных
        await db.execute(
            "INSERT INTO payments (amount, description, item_id) VALUES ($1, $2, $3)",
            payment.amount,
            payment.description,
            payment.item_id,
        )

    # Отправляем сообщение в RabbitMQ о совершенном платеже
    payment_data = {
        "amount": int(payment.amount),
        "description": payment.description,
        "item_id": int(payment.item_id),
    }

    message_body = json.dumps(payment_data)

    # Очередь, куда отправляет сообщения об оплате
    target_queue = "inventory_queue"

    await channel.default_exchange.publish(
        aio_pika.Message(body=message_body.encode("utf-8")),
        routing_key=target_queue
    )

    response_message = "Платеж успешно обработан"
    return {"message": response_message}
