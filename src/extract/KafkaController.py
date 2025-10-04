from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError
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
            admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            topic = NewTopic(
                name=self.topic_name,
                num_partitions=self.num_partitions,
                replication_factor=self.replication_factor
            )
            admin_client.create_topics([topic])
            return True
        except NoBrokersAvailable:
            self.logger.warning("No broker found, retrying in 5 seconds.")
            time.sleep(5)
            return False
        except TopicAlreadyExistsError:
            self.logger.info(f"Topic '{self.topic_name}' already exists.")
            return True

    def get_producer(self):
        retry_count = 0
        while retry_count < self.max_retries:
            if self.create_topic():
                try:
                    self.producer = KafkaProducer(
                        bootstrap_servers=self.bootstrap_servers,
                        value_serializer=lambda v: v.encode('utf-8')
                    )
                    return self.producer
                except Exception as e:
                    self.logger.error(f"Failed to create producer: {str(e)}")
                    retry_count += 1
                    time.sleep(5)
            else:
                retry_count += 1
                time.sleep(5)
        raise Exception("Failed to create Kafka producer after maximum retries.")
