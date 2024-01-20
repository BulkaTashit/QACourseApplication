from pydantic import BaseModel


class Item(BaseModel):
    item: str
    quantity: int
    price: int
