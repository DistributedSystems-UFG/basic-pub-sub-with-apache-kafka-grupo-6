from kafka import KafkaProducer
from const import *
from random import randint
from threading import Thread
import sys
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

#Argumento que define quantos topicos este produtor vai produzir
try:
  number_topics = int(sys.argv[1])
except:
  print ('Usage: python3 producer <number_of_topics>')
  exit(1)

producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

#Array de topicos
topics = []
#Loop para pegar os topicos
for x in range(number_topics):
    topic = input('Enter a new topic:\n')
    topics.append(topic)

#Loop pra enviar todos os topicos
for topic in topics:
    #Nova thread e criada para enviar cada topico
    thread = Thread(target=send_topic, args=(topic, producer))
    thread.start()
