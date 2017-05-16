import psycopg2
from kafka import KafkaConsumer
from kafka import TopicPartition
import time

if __name__ == '__main__':
	#consumer = KafkaConsumer('ho_insert_data_partition', group_id='g1', partition='0',bootstrap_servers='192.168.0.206:9092')
        consumer = KafkaConsumer(bootstrap_servers='192.168.0.206:9092', group_id='g1')
        consumer.assign([TopicPartition('ho_insert_data_partition',1)])


	conn = psycopg2.connect(dbname='segment_events',host='192.168.0.206',port='5432', user='xxxxx', password='xxxxx')
	cursor = conn.cursor()
	
	for message in consumer:
                        time.sleep(4)	
			print str(message.value) + '\n'
			cursor.execute(str(message.value))
			conn.commit()
