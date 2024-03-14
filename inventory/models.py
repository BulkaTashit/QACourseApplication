from pydantic import BaseModel
from fastapi_jwt_auth import AuthJWT


class User(BaseModel):
    username: str
    password: str


class Item(BaseModel):
    item: str
    quantity: int
    price: int


class Settings(BaseModel):
    authjwt_secret_key: str = "secret"
