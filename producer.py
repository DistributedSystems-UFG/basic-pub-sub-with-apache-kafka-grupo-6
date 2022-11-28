from kafka import KafkaProducer
from const import *
from random import randint
from threading import Thread
import time

#Funcao para enviar topicos
def send_topic(topic, producer):
    for i in range(100):
        msg = 'My ' + str(i) + 'st message for topic ' + topic
        print ('Sending message: ' + msg)
        producer.send(topic, value=msg.encode())
        #Produtor fica dormindo por um tempo aleatorio ate enviar uma nova mensagem
        time.sleep(randint(5, 10))

    producer.flush()

producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

#Loop pra ficar criando novos topicos
while (True):
    topic = print('Enter a new topic')
    #Nova thread e criada para enviar cada topico
    thread = Thread(target=send_topic, args=(topic, producer))
