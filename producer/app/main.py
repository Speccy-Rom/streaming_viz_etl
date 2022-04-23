import json
import logging
import os

from models.model import Purchase

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from kafka.errors import KafkaConnectionError

from dotenv import load_dotenv
load_dotenv()

KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER', "localhost:9092")

producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER)

app = FastAPI()

@app.on_event("startup")
async def start_kafka() -> None:
    print(f"Connecting to {KAFKA_BOOTSTRAP_SERVER}")
    try:
        await producer.start()
    except KafkaConnectionError:
        logging.error("Kafka not started!")
        if os.getenv('IGNORE_KAFKA'):
            return
        raise SystemExit("Kafka not ready!")

@app.on_event("shutdown")
async def stop_kafka() -> None:
    try:
        await producer.stop()
    except KafkaConnectionError:
        logging.error("Error shutting down Kafka!")

@app.post("/produce/{topicname}")
async def produce(purchase: Purchase, topicname: str) -> Purchase:
    json_str = json.dumps(jsonable_encoder(purchase))
    await producer.send_and_wait(topicname, json_str.encode())
    return purchase


@app.get("/")
async def root():
    return {"status": "200"}
