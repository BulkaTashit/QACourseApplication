from pydantic import BaseModel

class Payment(BaseModel):
    amount: float
    description: str
    item_id: int
