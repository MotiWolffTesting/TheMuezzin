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

## Threshold Justification

### Chosen Thresholds:
- **5% (Low)**: Catches content with isolated threatening terms
- **15% (Medium)**: Identifies content with systematic use of threatening language  
- **30% (High)**: Flags content with extensive threatening content requiring immediate attention

### Reasoning:
- **Balanced approach**: Sensitivity vs specificity
- **Based on content analysis**: Typical patterns in threatening content
- **Configurable**: Can be adjusted based on operational needs
- **Documented**: Clear justification for each threshold

## File structure

```
TheMuezzin/
├── README.md
├── docker-compose.yml
├── scripts/
│   └── commands.sh                 
├── shared/
│   ├── bds_analyzer.py             
│   ├── config.py                
│   └── logger.py                 
├── services/
│   ├── data_acceptance/           
│   │   ├── batch_processor.py      
│   │   ├── file_reader.py
│   │   ├── json_builder.py
│   │   ├── kafka_publisher.py
│   │   ├── metadata.py
│   │   ├── config.py
│   │   └── main.py                
│   ├── data_consuming/          
│   │   ├── elasticsearch_service.py
│   │   ├── kafka_consumer.py
│   │   ├── mongodb_service.py
│   │   ├── config.py
│   │   └── main.py
│   ├── data_transcriber/          
│   │   ├── mongodb_transcriber.py  
│   │   ├── transcriber.py
│   │   ├── config.py
│   │   └── main.py
│   ├── bds_analysis/              
│   │   ├── bds_storage.py
│   │   ├── kafka_consumer.py
│   │   ├── config.py
│   │   └── main.py
│   └── query_service/              
│       ├── main.py                
│       ├── config.py
│       └── requirements.txt
├── .gitignore
└── .env.example
```

## Running it

### Prerequisites
- Docker (for infra: Zookeeper, Kafka, MongoDB, Elasticsearch, Kibana)
- Python 3.11 (local services)

Create and activate a Python 3.11 virtual environment (first time only):
```bash
cd "/Users/mordechaywolff/Desktop/IDF/8200 Training/TheMuezzin"
python3.11 -m venv .venv311
source .venv311/bin/activate
pip install -r services/data_acceptance/requirements.txt \
            -r services/data_consuming/requirements.txt \
            -r services/data_transcriber/requirements.txt \
            -r services/bds_analysis/requirements.txt
```

### One-command local run (recommended)
The script brings up infra, waits for Elasticsearch, starts consumers, and runs the batch producer:
```bash
bash scripts/commands.sh
```

What it does:
- Starts Zookeeper, Kafka (advertised as `localhost:9092`), MongoDB, Elasticsearch, Kibana
- Waits for ES health to be green
- Starts `data_consuming`, `data_transcriber`, and `bds_analysis` in the background
- Runs `data_acceptance/batch_processor.py` to publish all WAVs from `/Users/mordechaywolff/Desktop/podcasts`
- Starts Query Service API on `http://localhost:8080`

### Manual run (advanced)
Infra (with localhost-advertised Kafka):
```bash
docker network create themuezzin-net 2>/dev/null || true

docker run -d --name zookeeper --network themuezzin-net -p 2181:2181 \
  -e ZOOKEEPER_CLIENT_PORT=2181 -e ZOOKEEPER_TICK_TIME=2000 \
  confluentinc/cp-zookeeper:latest

docker run -d --name kafka --network themuezzin-net -p 9092:9092 \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  confluentinc/cp-kafka:7.5.0

docker run -d --name mongodb --network themuezzin-net -p 27017:27017 mongo:latest

docker run -d --name elasticsearch --network themuezzin-net -p 9200:9200 \
  -e "discovery.type=single-node" -e "xpack.security.enabled=false" \
  -e "xpack.security.http.ssl.enabled=false" \
  -e "ES_JAVA_OPTS=-Xms512m -Xmx512m" \
  docker.elastic.co/elasticsearch/elasticsearch:8.11.0

# optional Kibana
docker run -d --name kibana --network themuezzin-net -p 5601:5601 \
  -e ELASTICSEARCH_HOSTS=http://elasticsearch:9200 \
  docker.elastic.co/kibana/kibana:8.11.0

# health
curl -s http://localhost:9200/_cluster/health
```

Services (in separate terminals; activate `.venv311` first):
```bash
# Data Consuming
cd services/data_consuming
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC_NAME=file_metadata_topic
export KAFKA_GROUP_ID=data-consuming-group
export MONGODB_ATLAS_URI=mongodb://localhost:27017
export MONGODB_DB_NAME=kafka_db
export MONGODB_COLLECTION_NAME=processed_messages
export ELASTICSEARCH_HOST=localhost
export ELASTICSEARCH_PORT=9200
export ELASTICSEARCH_INDEX=file_metadata
export LOGGER_ES_HOST=localhost:9200
export LOGGER_INDEX=muezzin_logs
python main.py

# Data Transcriber (also auto-publishes transcriptions to Kafka for BDS)
cd services/data_transcriber
export MONGODB_ATLAS_URI=mongodb://localhost:27017
export MONGODB_DB_NAME=kafka_db
export MONGODB_COLLECTION_NAME=processed_messages
export ELASTICSEARCH_HOST=localhost
export ELASTICSEARCH_PORT=9200
export ELASTICSEARCH_INDEX=file_metadata
export LOGGER_ES_HOST=localhost:9200
export LOGGER_INDEX=muezzin_logs
export SPEECH_RECOGNITION_LANGUAGE=en-US
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC_TRANSCRIPTION=audio_transcription
python main.py

# BDS Analysis (listens on 'audio_transcription')
cd services/bds_analysis
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC_TRANSCRIPTION=audio_transcription
export KAFKA_CONSUMER_GROUP_BDS=bds_analysis_group
export MONGODB_CONNECTION_STRING=mongodb://localhost:27017/
export MONGODB_DATABASE_NAME=muezzin_db
export MONGODB_COLLECTION_BDS=bds_analysis
export ELASTICSEARCH_HOST=localhost:9200
export ELASTICSEARCH_INDEX_BDS=bds_analysis
export LOGGER_ES_HOST=localhost:9200
export LOGGER_INDEX=muezzin_logs
python main.py

# Producer (batch over local WAV directory)
cd services/data_acceptance
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=file_metadata_topic
python batch_processor.py
```

### Verify
```bash
# metadata present
curl -s "http://localhost:9200/file_metadata/_search?size=1&pretty"
# transcription present
curl -s "http://localhost:9200/file_metadata/_search?q=transcription:*&size=1&pretty"
# BDS results
curl -s "http://localhost:9200/bds_analysis/_count" | jq .
```

### Why keep both single-file and batch ingestion?
- Single-file (`services/data_acceptance/main.py`):
  - Quick sanity checks for a specific WAV via `FILE_PATH`
  - Easier debugging (one message, focused logs)
  - Useful during development and demos
- Batch (`services/data_acceptance/batch_processor.py`):
  - Processes an entire folder of WAVs with retry/timeout logic
  - Best for realistic runs and backfilling datasets
  - Drives the full pipeline at scale

Both are kept to support fast iteration (single file) and production-like throughput (batch). The startup script uses batch by default; use single-file when you need to validate one input end-to-end.

### Query Service (API)
- Health: `curl -s http://localhost:8080/health`
- Files search: `curl -s "http://localhost:8080/search/files?q=<term>"`
- Transcriptions search: `curl -s "http://localhost:8080/search/transcriptions?q=<term>"`
- Recent files: `curl -s "http://localhost:8080/files/recent?size=10"`
- BDS filter: `curl -s "http://localhost:8080/bds/search?min_percentage=15&threat_level=low"`
- File by id: `curl -s "http://localhost:8080/files/<doc_id>"`
- Swagger UI: `http://localhost:8080/docs`

### Troubleshooting
- If Kafka clients fail to connect from host, ensure Kafka uses `KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092`.
- Logger errors like "ES log failed: Connection reset" at startup are benign while ES is booting.
- Ensure `.venv311` (Python 3.11) is active; `kafka-python` may not work reliably on 3.13.

