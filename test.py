from flask import Flask, request
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import json
import asyncio

async def consume():
    list = []
    consumer = AIOKafkaConsumer(
                "Orders",
                bootstrap_servers=['localhost:9092'],
                auto_offset_reset="earliest",
                value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    await consumer.start()
    try:
        async for msg in consumer:
            print("consumed: ", msg.topic, msg.partition, msg.offset,
                    msg.key, msg.value, msg.timestamp)
            list.append(msg.value)
            print(list)
    finally:
        await consumer.stop()

asyncio.run(consume())