from .RedditController import RedditClient
from .KafkaController import KafkaController
import logging.config
import time
import yaml
import json
from helpers import get_settings

with open("helpers/logging_config.yaml", "r") as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)

logger = logging.getLogger("extractor")

settings = get_settings()

def main():
    subreddit = settings.subreddit   # you can make this configurable
    topic_name = settings.topic_name
    # Init controllers
    reddit_client = RedditClient()
    kafka_controller = KafkaController(bootstrap_servers=settings.KAFKA_SERVER, topic_name=topic_name)
    producer = kafka_controller.get_producer()

    while True:
        try:
            posts = reddit_client.fetch_subreddit_posts(subreddit, limit=5)
            for category, post_list in posts.items():
                for post in post_list:
                    message = {
                        "category": category,
                        "title": post["title"],
                        "score": post["score"],
                        "url": post["url"],
                        "created_utc": post["created_utc"],
                        "body": post["post_body"],
                        "comments": post["comments"]
                    }
                    producer.send(topic_name, json.dumps(message))
                    logger.info(f"Sent post '{post['title'][:50]}...' to topic {topic_name}")
            
            producer.flush()  # ensure messages are sent
            time.sleep(60)  # wait before fetching again
        except Exception as e:
            logger.error(f"Pipeline error: {str(e)}", exc_info=True)
            time.sleep(10)


if __name__ == "__main__":
    main()