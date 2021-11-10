from flask import Flask, request
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import json
import asyncio
app = Flask(__name__)

sopiapas = 0

def serializer(value):
    return json.dumps(value).encode()

@app.route('/')
def main():
    return "Hello World!"

@app.route('/producer', methods = ['POST'])
async def produce():
    if request.method == 'POST':
        n_sopaipa = request.form['n_sopaipa']
        mail_vendedor = request.form['mail_vendedor']
        mail_cocinero = request.form['mail_cocinero']
        producer = AIOKafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=serializer)
        resp = {'numero_sopaipillas':n_sopaipa, 'mail_vendedor':mail_vendedor, 'mail_cocinero':mail_cocinero}
        await producer.start()
        try:
            await producer.send_and_wait("Orders", resp)
        finally:
            await producer.stop()
        return resp

@app.route('/consumer', methods = ['GET'])
async def consume():
    if request.method == 'GET':
        list = []
        consumer = AIOKafkaConsumer(
            'Orders',
            auto_offset_reset="earliest",
            bootstrap_servers=['localhost:9092'],
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


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)