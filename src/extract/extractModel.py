from pydantic import BaseModel, Field
from typing import List


class CommentModel(BaseModel):
    body: str             # The content of the comment
    score: int            # The comment's net score
    created_utc: float    # Unix timestamp of creation
    

class Posts(BaseModel):
    category :str
    title:str
    score:int
    url:str
    created_utc: float
    post_body: str = Field(alias='post_body') # The body of the post ('selftext') 
    comments : List[CommentModel]



