In order to get Spark to send data to Cassandra, you need to setup your keyspace and tables first! 
## Run these commands:
1 . Create the keyspace with a meaningful name
```bash
CREATE KEYSPACE IF NOT EXISTS reddit
WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
```
2. Use your key space
```bash
USE reddit;
```
3. Intialise Tables
```bash
CREATE TABLE IF NOT EXISTS raw_table (
    id UUID PRIMARY KEY,
    category TEXT,
    title TEXT,
    score INT,
    url TEXT,
    created_utc FLOAT,
    post_body TEXT,
    comments TEXT
);

CREATE TABLE IF NOT EXISTS silver_table (
    id UUID PRIMARY KEY,
    category TEXT,
    title TEXT,
    score INT,
    comments TEXT,
    url TEXT,
    post_body TEXT,
    created TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold_table (
    id UUID PRIMARY KEY,
    category TEXT,
    title TEXT,
    score INT,
    comments TEXT,
    Cleaned_post_body TEXT,
    Cleaned_post_body_tokens LIST<TEXT>
);
```
4.  
```bash
$ 
```