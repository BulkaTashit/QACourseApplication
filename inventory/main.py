import asyncio
import json
import logging
import asyncpg
import aio_pika

from aio_pika import IncomingMessage
from fastapi import FastAPI, Depends, HTTPException
from fastapi_jwt_auth import AuthJWT
from fastapi_jwt_auth.exceptions import MissingTokenError

from models import Item, User, Settings
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
            await delete_items_quantity(item_id, amount)

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
        except aio_pika.exceptions.CONNECTION_EXCEPTIONS:
            print("Waiting for RabbitMQ to start...")
            await asyncio.sleep(1)

    channel = await connection.channel()
    queue = await channel.declare_queue(RABBITMQ_QUEUE, durable=True)

    print("Setting up RabbitMQ consumer")
    await queue.consume(on_message)


@AuthJWT.load_config
def get_config():
    return Settings()


@app.post('/auth')
async def auth(user: User, Authorize: AuthJWT = Depends(), db=Depends(get_database)):
    # Проверка имени пользователя и пароля ()
    async with db.transaction():
        user_db = await db.fetchrow("SELECT * FROM auth WHERE login = $1", user.username)

        if user_db is None or user_db["password"] != user.password:
            raise HTTPException(status_code=401, detail="Неверное имя пользователя или пароль")

    # Создание токена
    access_token = Authorize.create_access_token(subject=user.username)
    return {"access_token": access_token}


@app.post("/items/")
async def create_item(item: Item, db=Depends(get_database), Authorize: AuthJWT = Depends()):
    try:
        # Проверяем наличие JWT токена
        Authorize.jwt_required()
    except MissingTokenError:
        # Если токен отсутствует, возвращаем ошибку "Вы не авторизованы"
        raise HTTPException(status_code=401, detail="Вы не авторизованы")

        # Получаем подтвержденные данные из JWT
    current_user = Authorize.get_jwt_subject()

    # Проверяем наличие пользователя в JWT
    if not current_user:
        raise HTTPException(status_code=401, detail="Вы не авторизованы")

    async with db.transaction():
        result = await db.fetchval(
            "INSERT INTO items (item, quantity, price) VALUES ($1, $2, $3) RETURNING id",
            item.item,
            item.quantity,
            item.price,
        )
        print(f"ProductAdded with id {result}")
        items_id = result

    message_body = f"ProductAdded: {item.item}, {item.quantity}, {item.price}"

    response_message = {"id": items_id, "status": "success", "message": message_body}
    return response_message


@app.delete("/items/{item_id}")
async def delete_item(item_id: int, db=Depends(get_database), Authorize: AuthJWT = Depends()):
    try:
        # Проверяем наличие JWT токена
        Authorize.jwt_required()
    except MissingTokenError:
        # Если токен отсутствует, возвращаем ошибку "Вы не авторизованы"
        raise HTTPException(status_code=401, detail="Вы не авторизованы")

    # Получаем подтвержденные данные из JWT
    current_user = Authorize.get_jwt_subject()

    # Проверяем наличие пользователя в JWT
    if not current_user:
        raise HTTPException(status_code=401, detail="Вы не авторизованы")

    async with db.transaction():
        # Проверяем, существует ли ресурс с указанным ID
        existing_item = await db.execute("DELETE FROM items WHERE id = $1", item_id)
        if not existing_item:
            raise HTTPException(status_code=404, detail=f"Ресурса с ID {item_id} не найдено")

    response_message = {"status": "success", "message": f"Ресурс с ID {item_id} успешно удален"}
    return response_message


@app.delete("/items/delete_items_quantity/{item_id}")
async def delete_items_quantity(item_id: int, quantity_to_reduce: int = 1, Authorize: AuthJWT = Depends()):

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
async def set_item_quantity(item_id: int, quantity: int, db=Depends(get_database)):
    if quantity < 0:
        raise HTTPException(status_code=400, detail="Количество товара должно быть неотрицательным числом")

    async with db.transaction():
        # Устанавливаем количество товара в базе данных
        await db.execute("UPDATE items SET quantity = $1 WHERE id = $2", quantity, item_id)

        response_message = f"Количество товара с идентификатором {item_id} установлено равным {quantity}"
        return {"message": response_message}

@app.get("/items/")
async def read_items(q: str = None, limit: int = 10):
    return {"query": q, "limit": limit}



@app.get("/items/{item_id}")
async def get_item_by_id(item_id: int, db=Depends(get_database), Authorize: AuthJWT = Depends()):
    try:
        # Проверяем наличие JWT токена
        Authorize.jwt_required()
    except MissingTokenError:
        # Если токен отсутствует, возвращаем ошибку "Вы не авторизованы"
        raise HTTPException(status_code=401, detail="Вы не авторизованы")

        # Получаем подтвержденные данные из JWT
    current_user = Authorize.get_jwt_subject()

    # Проверяем наличие пользователя в JWT
    if not current_user:
        raise HTTPException(status_code=401, detail="Вы не авторизованы")
    # Получаем информацию о товаре по его идентификатору
    query = "SELECT id, item, quantity, price FROM items WHERE id = $1"

    try:
        result = await db.fetchrow(query, item_id)
    except asyncpg.exceptions.PostgresError as e:
        # Обработка ошибок PostgreSQL
        raise HTTPException(status_code=500, detail=f"Ошибка при выполнении запроса: {str(e)}")

    # Проверяем, существует ли товар с указанным идентификатором
    if not result:
        raise HTTPException(status_code=404, detail="Товар с указанным идентификатором не найден")

    # Формируем ответ
    response_data = {
        "id": result["id"],
        "item": result["item"],
        "quantity": result["quantity"],
        "price": result["price"]
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
    await asyncio.sleep(10)

    await db.execute("""
                        CREATE TABLE IF NOT EXISTS auth (
                            id serial PRIMARY KEY,
                            login text,
                            password text
                    )
                """)
    print("Database setup complete")

    asyncio.create_task(consume_queue())


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)
