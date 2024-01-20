import asyncpg

async def get_database():
    db_url = "postgresql://myuser:mypassword@postgres-container/mydatabase"
    connection = await asyncpg.connect(db_url)
    return connection