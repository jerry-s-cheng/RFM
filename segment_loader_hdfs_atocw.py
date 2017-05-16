import psycopg2
import time
import os
import time
from time import mktime
from datetime import datetime
from boto.s3.connection import S3Connection
from kafka import KafkaProducer


def is_processed(obj_name):
    sql = "select file_name from "+ schema_name +".loading_to_hdfs_log where file_name = '" + obj_name + "'"
    cursor.execute(sql)
    if cursor.rowcount > 0:
        return True
    else:
        return False

def get_the_latest_modified_date():
    sql = "select max(last_modified_at) as latest_modified_date from "+ schema_name +".loading_to_hdfs_log"
    print sql
    cursor.execute(sql)
    record = cursor.fetchone()
    if record[0] is None:
        return '2015-01-01 00:00:01'
    else:
        return str(record[0])

def save_the_latest_modified_date(object_name, date_str):
    sql = "insert into "+ schema_name +".loading_to_hdfs_log (file_name, last_modified_at) values " \
          "('" + object_name + "','" + date_str + "')"
    print sql
    cursor.execute(sql)
    conn.commit()

def download_s3_obj_and_sync_to_hdfs(file_name):
    key = bucket.get_key(file_name)
    temp_str = file_name.split('/AfiTgWFG8T/')
    temp_str1 = temp_str[1].split('/')
    obj_name = temp_str1[1]
    print 'obj_name - ' + obj_name
    key.get_contents_to_filename(raw_data_folder + obj_name)
    
    os.system('gunzip ' + raw_data_folder + obj_name)
    unzip_obj_name = obj_name.rstrip('.gz')

    sync_to_hdfs(raw_data_folder + unzip_obj_name)
    send_msg_to_broker(unzip_obj_name)

def sync_to_hdfs(obj_full_path):
    print '1111'
    hdfs_command_str = 'hadoop fs -copyFromLocal ' + obj_full_path + ' ' + hdfs_path
    print '2222'
    time.sleep(3)
    print hdfs_command_str
    os.system(hdfs_command_str)
    print '3333'

def send_msg_to_broker(obj_name):
    #print '4444'
    obj_name_str = store + '***' + obj_name
    producer.send(topic, bytes(obj_name_str))
    #print '5555'

if __name__ == '__main__':
    AWS_KEY = 'xxxxx'
    AWS_SECRET = 'xxxxx'
    raw_data_folder = '/home/hduser/raw_data/segment/atocw_production/'
    hdfs_path = '/segment/atocw_production'
    schema_name = 'atocw_production'
    producer = KafkaProducer(bootstrap_servers='192.168.0.206:9092')
    topic = "ho_production"
    store = 'segment-logs/AfiTgWFG8T'  # atocw_production
    conn = psycopg2.connect(dbname='segment_events',
                            host='192.168.0.206',
                            port='5432', user='xxxxx', password='xxxxx')
    cursor = conn.cursor()
    aws_connection = S3Connection(AWS_KEY, AWS_SECRET)
    bucket = aws_connection.get_bucket('xxxxx-xxxxx-logs', validate=False)
    
    while True:
      latest_modified_date = get_the_latest_modified_date()
      print 'latest_modified_date from DB ' + latest_modified_date
      i = 0
      for s3_object in bucket:
          file_name = s3_object.name
          modified = time.strptime(s3_object.last_modified, '%Y-%m-%dT%H:%M:%S.%fZ')
          last_modified_ts = str(datetime.fromtimestamp(mktime(modified)))

          if store in file_name and last_modified_ts > latest_modified_date:#and is_processed(file_name) is False:
              i = i + 1
              if i < 10000000:
                  print str(
                      i) + '--' + file_name + '  -- ' + last_modified_ts + '  latest_modified_date' + latest_modified_date

                  download_s3_obj_and_sync_to_hdfs(file_name)
                  save_the_latest_modified_date(file_name, last_modified_ts)
              else:
                  break

      print i - 1
      time.sleep(600)

    cursor.close()
    conn.close()
