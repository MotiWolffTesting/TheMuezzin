# The Muezzin - Deployment Commands

# Create Docker Network
docker network create themuezzin-net 2>/dev/null || echo "Network already exists"

# Stop and remove existing containers if they exist
docker stop zookeeper kafka mongodb elasticsearch kibana 2>/dev/null || true
docker rm zookeeper kafka mongodb elasticsearch kibana 2>/dev/null || true

# Zookeeper
docker run -d \
  --name zookeeper \
  --network themuezzin-net \
  -p 2181:2181 \
  -e ALLOW_ANONYMOUS_LOGIN=yes \
  bitnami/zookeeper:latest

# Kafka
docker run -d \
  --name kafka \
  --network themuezzin-net \
  -p 9092:9092 \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
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

# Kibana
docker run -d \
  --name kibana \
  --network themuezzin-net \
  -p 5601:5601 \
  -e ELASTICSEARCH_HOSTS=http://elasticsearch:9200 \
  docker.elastic.co/kibana/kibana:8.11.0


# Wait for Elasticsearch health
printf "Waiting for Elasticsearch to be ready..."
for i in {1..30}; do
  if curl -s http://localhost:9200/_cluster/health >/dev/null; then echo " OK"; break; fi
  printf "."; sleep 1
done

# Start Python services (consumers first) in background
cd services/data_consuming
  export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
  export KAFKA_TOPIC_NAME=file_metadata_topic
  export KAFKA_GROUP_ID=data-consuming-group
  export MONGODB_ATLAS_URI=mongodb://localhost:27017
  export ELASTICSEARCH_HOST=localhost
  export ELASTICSEARCH_PORT=9200
  export LOGGER_ES_HOST=localhost:9200
  export LOGGER_INDEX=muezzin_logs
  nohup python main.py >/tmp/data_consuming.log 2>&1 &

cd ../data_transcriber
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
  nohup python main.py >/tmp/data_transcriber.log 2>&1 &

cd ../bds_analysis
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
  nohup python main.py >/tmp/bds_analysis.log 2>&1 &

cd ../../

# To run the Python services locally

# Data Acceptance Batch (producer)
cd services/data_acceptance
   export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   export KAFKA_TOPIC=file_metadata_topic
   python batch_processor.py

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
  export LOGGER_ES_HOST=localhost:9200
  export LOGGER_INDEX=muezzin_logs
  python main.py

# Health check for Elasticsearch
curl -s http://localhost:9200/_cluster/health | jq '.' 2>/dev/null || echo "Elasticsearch not ready yet"


