import aio_pika


async def get_rabbit_channel():
    connection = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq-container")
    channel = await connection.channel()
    return channel
