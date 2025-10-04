from kafka import KafkaProducer
import praw
from dotenv import load_dotenv
from datetime import datetime
from urllib.error import HTTPError
import logging
import os
print(os.getcwd())
from helpers import get_settings
import time
from kafka.errors import NoBrokersAvailable
from pydantic import ValidationError


logger = logging.getLogger(__name__)


class RedditClient:
    

    def __init__(self):
        try:
            settings = get_settings()
            self.reddit = praw.Reddit(
                client_id=settings.client_id_env,
                client_secret=settings.client_secret_env,
                user_agent=settings.user_agent_env
            )
        except ValidationError as e:
            logger.error(f"Missing Reddit credentials: {e}")
            raise RuntimeError("Reddit credentials are missing or invalid in .env file") from e
        
    
    def fetch_subreddit_posts(self, subreddit_name, limit=5):
        subreddit = self.reddit.subreddit(subreddit_name)

        results = {
            "new": [],
            "hot": []
        }

        def process_post(post):
            """Extracts info and valid comments from a submission"""
            post_info = {
                "title": post.title,
                "score": post.score,
                "url": post.url,
                "created_utc": post.created_utc,
                "post_body": post.selftext,
                "comments": []
            }

            post.comments.replace_more(limit=0)  # Expand all comments, no "load more"
            for comment in post.comments.list():
                if comment.author and "bot" not in comment.author.name.lower():  # skip bots
                    post_info["comments"].append({
                        "body": comment.body,
                        "score": comment.score,
                        "created_utc": comment.created_utc
                    })

            return post_info

        for post in subreddit.new(limit=limit):
            results["new"].append(process_post(post))

        for post in subreddit.hot(limit=limit):
            results["hot"].append(process_post(post))

        return results