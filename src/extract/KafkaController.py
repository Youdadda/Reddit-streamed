from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer
import time
import logging

class KafkaController:
    def __init__(self, bootstrap_servers, topic_name, num_partitions=3, replication_factor=1, max_retries=3):
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.max_retries = max_retries
        self.producer = None
        self.logger = logging.getLogger(__name__)

    def create_topic(self):
        try:
            admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})
            new_topic = NewTopic(self.topic_name, num_partitions=self.num_partitions, replication_factor=self.replication_factor)
            fs = admin_client.create_topics([new_topic])
            # Wait for each operation to finish
            for topic, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    self.logger.info(f"Topic '{topic}' created.")
                except Exception as e:
                    if 'TOPIC_ALREADY_EXISTS' in str(e) or 'already exists' in str(e):
                        self.logger.info(f"Topic '{topic}' already exists.")
                        return True
                    else:
                        self.logger.error(f"Failed to create topic '{topic}': {e}")
                        return False
            return True
        except Exception as e:
            self.logger.warning(f"Error creating topic: {e}, retrying in 5 seconds.")
            time.sleep(5)
            return False

    def get_producer(self):
        retry_count = 0
        while retry_count < self.max_retries:
            if self.create_topic():
                try:
                    self.producer = Producer({'bootstrap.servers': self.bootstrap_servers})
                    return self.producer
                except Exception as e:
                    self.logger.error(f"Failed to create producer: {str(e)}")
                    retry_count += 1
                    time.sleep(5)
            else:
                retry_count += 1
                time.sleep(5)
        raise Exception("Failed to create Kafka producer after maximum retries.")
