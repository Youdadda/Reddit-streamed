# Reddit ETL Pipeline

A robust data pipeline that extracts Reddit posts and comments, processes them through multiple transformation layers, and prepares them for political sentiment analysis.

## Architecture

### Data Flow
1. **Extract**: Reddit API → Kafka
2. **Transform**: 
   - Bronze (Raw): Direct ingestion from Kafka
   - Silver: Cleaned data with normalized timestamps and filtered comments
   - Gold: NLP-ready data with tokenized text and removed stop words

### Tech Stack
- Apache Kafka
- Apache Spark Structured Streaming
- Apache Cassandra
- Docker & Docker Compose
- Python (pyspark, praw)

## Project Structure
```
Reddit-ETL/
├── src/
│   ├── extract/           # Reddit data extraction
│   ├── processor/         # Spark processing jobs
│   │   ├── dependencies/  # Spark configuration
│   │   ├── streams/      # Streaming queries
│   │   └── utilities/    # Transformations & schemas
│   └── helpers/          # Configuration & logging
├── docker/               # Container definitions
└── Makefile             # Automation scripts
```

## Features
- Concurrent streaming processing (Bronze, Silver, Gold)
- Robust error handling and logging
- Configurable data extraction
- Production-ready containerization
- NLP preprocessing for political sentiment analysis

## Getting Started

### Prerequisites
- Docker and Docker Compose
- Python 3.8+
- Reddit API credentials

### Setup
1. Clone the repository
2. Create `.env` file with Reddit and Kafka credentials
3. Build and start services:
```bash
docker-compose up -d
```

### Running the Pipeline
```bash
# From the src directory
make run
```

## Development

The pipeline uses a Makefile for common operations:
- `make clean-remote`: Clean previous deployments
- `make deploy-files`: Deploy processor files
- `make prepare-scripts`: Fix line endings
- `make submit-job`: Submit Spark job
- `make run`: Execute all steps

## Monitoring
- Spark UI: http://localhost:8080
- Kafka Manager: http://localhost:9000

## Next Steps
- [ ] Add schema evolution support
- [ ] Implement ML pipeline for sentiment analysis
- [ ] Add monitoring dashboards
- [ ] Scale worker nodes