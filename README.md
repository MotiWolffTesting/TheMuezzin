# The Muezzin - Audio Processing Pipeline

A microservices-based system for processing audio files, extracting metadata, and generating transcriptions. Built with Python and designed to handle batch processing of WAV audio files.

## What it does

This system takes audio files (specifically WAV format), extracts their metadata, stores them in databases, and converts speech to text. It's split into three main services that work together through Kafka messaging.

## Architecture

### Services
- **data_acceptance**: Reads audio files, extracts metadata using TinyTag, and sends messages to Kafka
- **data_consuming**: Receives messages from Kafka and stores data in MongoDB + metadata in Elasticsearch  
- **data_transcriber**: Processes audio files and generates text transcriptions

### Infrastructure
- **Kafka**: Message broker for async communication between services
- **MongoDB**: Stores audio files as binary data with metadata
- **Elasticsearch**: Indexes metadata for fast searching

## Why these libraries?

**TinyTag** - Lightweight metadata extraction for audio files. No heavy dependencies, just gets the basic info we need (duration, filesize, etc.)

**SpeechRecognition** - Wrapper around multiple speech-to-text APIs. Gives flexibility to switch providers without code changes.

**pydub** - Audio processing library. Needed to convert audio to the right format (mono, 16kHz) for speech recognition.

## Design decisions

**Microservices approach**: Each service does one thing. Makes it easier to debug and scale individual parts.

**Kafka for messaging**: Services don't talk directly to each other. This way if one service is slow or crashes, others keep working.

**Dual storage**: MongoDB for raw data + Elasticsearch for searching. Each database does what it's best at.

**Centralized logging**: All services log to the same Elasticsearch instance. Makes debugging much easier when tracing requests.

**Configuration with dataclasses**: Type-safe config loading from environment variables. Better than using raw dictionaries.

The `sys.path.append` approach for shared modules isn't the most "Pythonic" way, but it's simple and works for this project size.

## File structure

```
services/
├── data_acceptance/     # File ingestion and metadata
├── data_consuming/      # Message processing and storage  
├── data_transcriber/    # Speech-to-text processing
└── shared/             # Common utilities (logging)
```

## Running it

Start the infrastructure services individually:

```bash
# Create Docker Network
docker network create themuezzin-net 2>/dev/null || echo "Network already exists"

# Zookeeper
docker run -d \
  --name zookeeper \
  --network themuezzin-net \
  -p 2181:2181 \
  -e ZOOKEEPER_CLIENT_PORT=2181 \
  -e ZOOKEEPER_TICK_TIME=2000 \
  confluentinc/cp-zookeeper:latest

# Kafka
docker run -d \
  --name kafka \
  --network themuezzin-net \
  -p 9092:9092 \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  confluentinc/cp-kafka:7.5.0

# MongoDB
docker run -d \
  --name mongodb \
  --network themuezzin-net \
  -p 27017:27017 \
  mongo:latest

# Elasticsearch
docker run -d \
  --name elasticsearch \
  --network themuezzin-net \
  -p 9200:9200 \
  -e "discovery.type=single-node" \
  -e "xpack.security.enabled=false" \
  -e "xpack.security.http.ssl.enabled=false" \
  -e "ES_JAVA_OPTS=-Xms512m -Xmx512m" \
  docker.elastic.co/elasticsearch/elasticsearch:8.11.0

# Kibana (optional, for ES visualization)
docker run -d \
  --name kibana \
  --network themuezzin-net \
  -p 5601:5601 \
  -e ELASTICSEARCH_HOSTS=http://elasticsearch:9200 \
  kibana:8.11.0
```

Now run the local project

```bash
# Data Acceptance Service:
cd services/data_acceptance
   export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   export KAFKA_TOPIC_NAME=file_metadata_topic
   export LOGGER_ES_HOST=localhost:9200
   export LOGGER_INDEX=muezzin_logs
   python main.py

# Data Consuming Service:
cd services/data_consuming
  export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
  export KAFKA_TOPIC_NAME=file_metadata_topic
  export KAFKA_GROUP_ID=data-consuming-group
  export MONGODB_ATLAS_URI=mongodb://localhost:27017
  export ELASTICSEARCH_HOST=localhost
  export ELASTICSEARCH_PORT=9200
  export LOGGER_ES_HOST=localhost:9200
  export LOGGER_INDEX=muezzin_logs
  python main.py

# Data Transcriber Service:
cd services/data_transcriber
  export MONGODB_ATLAS_URI=mongodb://localhost:27017
  export MONGODB_DB_NAME=kafka_db
  export MONGODB_COLLECTION_NAME=collection
  export ELASTICSEARCH_HOST=localhost
  export ELASTICSEARCH_PORT=9200
  export ELASTICSEARCH_INDEX=file_metadata
  export LOGGER_ES_HOST=localhost:9200
  export LOGGER_INDEX=muezzin_logs
  export SPEECH_RECOGNITION_LANGUAGE=en-US
  python main.py
   
```

Check if everything is running:
```bash
# Elasticsearch health
curl http://localhost:9200/_cluster/health
```


Then run the Python services locally. The batch processor handles multiple files at once with retry logic for Kafka timeouts. It processes files from `/Users/mordechaywolff/Desktop/podcasts`.

