from kafka import KafkaConsumer
import hdfs

def load_data():
    consumer = KafkaConsumer(
        'input_topic',
        bootstrap_servers='kafka-broker:9092',
        auto_offset_reset='earliest'
    )
    
    client = hdfs.InsecureClient("http://namenode:50070")
    
    with client.write('/data/input/raw_data.csv') as writer:
        for message in consumer:
            writer.write(message.value + b'\n')

if __name__ == "__main__":
    load_data()
