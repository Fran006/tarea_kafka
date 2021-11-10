from flask import Flask, request
from kafka import KafkaConsumer, KafkaProducer
import json
import threading
app = Flask(__name__)

consumer = KafkaConsumer('Orders',
                        bootstrap_servers=['localhost:9092'],
                        value_deserializer=lambda m: json.loads(m.decode('ascii')))

producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'), bootstrap_servers=['localhost:9092'])


@app.route('/')
def main():
    return "Hello World!"

@app.route('/producer', methods=['POST'])
def produce():
    if request.method=="POST":
        n_sopaipa = request.form['n_sopaipa']
        mail_vendedor = request.form['mail_vendedor']
        mail_cocinero = request.form['mail_cocinero']
        producer.send('Orders', {'numero_sopaipillas':n_sopaipa, 'mail_vendedor':mail_vendedor, 'mail_cocinero':mail_cocinero})
        resp = {'numero_sopaipillas':n_sopaipa, 'mail_vendedor':mail_vendedor, 'mail_cocinero':mail_cocinero}
        producer.flush()
        return resp

@app.route('/consumer', methods=['GET'])
def consume():
    if request.method=="GET":
        list = []
        for message in consumer:
            list.append(message.value)
            print(message.value)
        return list


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)