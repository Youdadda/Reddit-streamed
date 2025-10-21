from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')

    client_id_env : str
    client_secret_env : str
    user_agent_env : str
    KAFKA_SERVER: str
    topic_name:str
    subreddit:str
    

def get_settings():
    return Settings(_env_file='.env', _env_file_encoding='utf-8')