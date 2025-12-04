#!/usr/bin/env python

from random import choice
from confluent_kafka import Producer

if __name__ == '__main__':

    config = {
        # Servidor BootStrap, Usuário e Senha
        'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
        'sasl.username':     '443FUC7CAQVLZQQO',
        'sasl.password':     'cfltnrg16R7F6f/uh8dX2BMZVNXFbYhlEEqAEpz1IasN86hZy+KbxcHNMEkjuEYA',

        # Propriedades Padrão do Confluent
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms':   'PLAIN',
        
        # Tempo de espera de resposta antes de cancelar a conexão
        'session.timeout.ms': 45000
    }

    # Cria uma instância de produtor
    producer = Producer(config)
    
    # Nome do Tópico
    topic = "vendas_online"

    # Função opcional de devolução.
    # É chamada por poll() e flush()
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Mensagem de Erro: {}'.format(err))
        else:
            print("Produzindo Evento no Tópico {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    # Produz dados combinando valores das listas aleatoriamente.
    user_ids = ['psilva', 'smanoel', 'jpires', 'jbernardo', 'hmoraes', 'fmilagres','irineu','zedamanga','pó','marquinhos','osvaldos']
    products = ['Camisa do Reinaldo', 'Rádio Relogio', 'Bermuda Praia', 'Mouse', 'Teclado','Chocolate','Carro','Casa','Cachorro','Panela','Lampada','Cabelo']

    count = 0
    for _ in range(10):
        user_id = choice(user_ids)
        product = choice(products)
        producer.produce(topic, product, user_id, callback=delivery_callback)
        count += 1

    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()