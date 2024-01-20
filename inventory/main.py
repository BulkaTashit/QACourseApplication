import asyncio
import json
import logging

import aio_pika

from aio_pika import IncomingMessage
from fastapi import FastAPI, Depends, HTTPException
from models import Item
from database import get_database

app = FastAPI()

# Параметры подключения к RabbitMQ
RABBITMQ_HOST = 'rabbitmq-container'
RABBITMQ_PORT = 5672
RABBITMQ_QUEUE = 'inventory_queue'
RABBITMQ_USER = 'guest'
RABBITMQ_PASSWORD = 'guest'


async def delete_quantity_message(message: IncomingMessage):
    """
    Обработчик сообщений из очереди RabbitMQ.
    """
    try:
        payload = message.body.decode("utf-8")
        logging.info(f"Received message: {payload}")

        try:
            # Преобразование JSON-строки в словарь
            payment_data = json.loads(payload)

            amount = int(payment_data["amount"])
            item_id = int(payment_data["item_id"])

            # Код для обработки платежа
            await delete_item(item_id, amount)

        except json.JSONDecodeError as json_error:
            logging.warning(f"Invalid JSON format. Skipping processing. {payload} {type(payload)}")
        except KeyError as key_error:
            logging.warning(f"Missing key in JSON. Skipping processing. {payload} {type(payload)}")

    except Exception as e:
        logging.error(f"Error processing message: {e}")


async def on_message(message: aio_pika.IncomingMessage):
    async with message.process():
        await delete_quantity_message(message)


async def consume_queue():
    # Подключение к RabbitMQ
    try:
        connection = await aio_pika.connect_robust(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            login=RABBITMQ_USER,
            password=RABBITMQ_PASSWORD,
        )
        print("Connection to RabbitMQ successful.")

        channel = await connection.channel()
        queue = await channel.declare_queue(RABBITMQ_QUEUE, durable=True)

        print("Setting up RabbitMQ consumer")

        await queue.consume(on_message)

    except Exception as e:
        print(f"Error connecting to RabbitMQ: {e}")


@app.post("/items/")
async def create_item(item: Item, db=Depends(get_database)):
    async with db.transaction():
        result = await db.execute(
            "INSERT INTO items (item, quantity, price) VALUES ($1, $2, $3) RETURNING id",
            item.item,
            item.quantity,
            item.price,
        )
        print(result)

    message_body = f"ProductAdded: {item.item}, {item.quantity}, {item.price}"

    response_message = "Товар успешно добавлен"
    return {"message": response_message}


@app.delete("/items/{item_id}")
async def delete_item(item_id: int, quantity_to_reduce: int = 1):
    db = await get_database()
    async with db.transaction():
        # Получаем текущее количество товара
        current_quantity = await db.fetchval("SELECT quantity FROM items WHERE id = $1", item_id)

        # Проверяем, достаточно ли товара для уменьшения
        if not current_quantity or int(current_quantity) < quantity_to_reduce:
            raise HTTPException(status_code=400, detail="Недостаточное количество товара для удаления")

        # Уменьшаем количество товара в базе данных
        new_quantity = max(0, int(current_quantity) - quantity_to_reduce)
        await db.execute("UPDATE items SET quantity = $1 WHERE id = $2", new_quantity, item_id)

        response_message = f"Количество товара с идентификатором {item_id} успешно уменьшено на {quantity_to_reduce}"
        return {"message": response_message}


@app.put("/items/{item_id}")
async def increase_item_quantity(item_id: int, quantity_to_add: int, db=Depends(get_database)):
    if quantity_to_add <= 0:
        raise HTTPException(status_code=400, detail="Количество для добавления должно быть положительным числом")

    async with db.transaction():
        # Увеличиваем количество товара в базе данных
        await db.execute("UPDATE items SET quantity = quantity + $1 WHERE id = $2", quantity_to_add, item_id)

        response_message = f"Количество товара с идентификатором {item_id} успешно увеличено на {quantity_to_add}"
        return {"message": response_message}


@app.get("/items/{item_id}")
async def get_item_by_id(item_id: int, db=Depends(get_database)):
    async with db.transaction():
        # Получаем информацию о товаре по его идентификатору
        result = await db.execute("SELECT id, item, quantity, price FROM items WHERE id = $1", item_id)
        item = result.fetchone()

        # Проверяем, существует ли товар с указанным идентификатором
        if not item:
            raise HTTPException(status_code=404, detail="Товар с указанным идентификатором не найден")

        # Формируем ответ
        response_data = {
            "id": item[0],
            "item": item[1],
            "quantity": item[2],
            "price": item[3]
        }

        return response_data


@app.on_event("startup")
async def startup():
    db = await get_database()
    await db.execute("""
                        CREATE TABLE IF NOT EXISTS items (
                            id serial PRIMARY KEY,
                            item text,
                            quantity int,
                            price numeric
                    )
                """)
    print("Database setup complete")

    await asyncio.sleep(10)

    asyncio.create_task(consume_queue())

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)

