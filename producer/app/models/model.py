from pydantic import BaseModel
from datetime import datetime

class Purchase(BaseModel):
    purchase_id: str
    stock_code: int
    item_description: str
    quantity: int
    customer_id: str
    cost: float
    purchase_date: datetime
