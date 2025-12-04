#!/usr/bin/env python

from confluent_kafka import Consumer

if __name__ == '__main__':

    config = {
        # Servidor BootStrap, Usuário e Senha
        'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
        'sasl.username':     '443FUC7CAQVLZQQO',
        'sasl.password':     'cfltnrg16R7F6f/uh8dX2BMZVNXFbYhlEEqAEpz1IasN86hZy+KbxcHNMEkjuEYA',

        # Propriedades Padrão do Confluent
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms':   'PLAIN',
        
        # Tempo de espera de entre cada tentativa de consumir um mensagem
        'session.timeout.ms': 45000,
        
        # Identificador do ID Grupo
        'group.id': 'kafka-python-aprendendo',
        
        # Configuração padrão para recuperação
        'auto.offset.reset': 'earliest'
    }

    # Cria uma instância de Consumidor
    consumer = Consumer(config)

    # Inscrição em um Tópico
    topic = "vendas_online"
    
    consumer.subscribe([topic])

    # Poll para novas mensagens do Kafka e Imprimindo Elas.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Aguardando...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                # Extraí Key e Value (Chave e Valor) e Imprimi. Faz a decodificação da Mensagem.
                print("Consumindo Evento do Tópico {topic}: chave = {key:12} Valor = {value:12}".format(
                    topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
    except KeyboardInterrupt:
        print("Programa interrompido pelo Usuário Via Teclado!!")
    finally:
        # Deixa o Grupo de commit offsets finais
        consumer.close()