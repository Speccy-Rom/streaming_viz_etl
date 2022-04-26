import pytest
from datetime import datetime

from httpx import AsyncClient
from app.main import app
from app.models.model import Purchase

from fastapi.encoders import jsonable_encoder


@pytest.mark.anyio
async def test_root() -> None:
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.get("/test/")
    assert response.status_code == 200
    assert response.json() == {"hello": "world!"}


@pytest.mark.anyio
async def test_produce() -> None:
    p = Purchase(
        purchase_id=1,
        stock_code=1,
        item_description="1",
        quantity=1,
        customer_id=1,
        cost=1,
        purchase_date=datetime.now(),
    )
    json_encoded = jsonable_encoder(p)
    async with AsyncClient(
        app=app, base_url="http://test") as ac:
        response = await ac.post("/produce/123", json=json_encoded)
    assert response.json() == json_encoded



from fastapi.testclient import TestClient
def test_read_items():
    with TestClient(app) as client:
        response = client.get("/test/")
        assert response.status_code == 200


@pytest.mark.anyio
async def test_read_items_aysnc():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.get("/test/")
    assert response.status_code == 200
