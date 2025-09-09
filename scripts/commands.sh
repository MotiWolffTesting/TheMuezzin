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

# Kibana
docker run -d \
  --name kibana \
  --network themuezzin-net \
  -p 5601:5601 \
  -e ELASTICSEARCH_HOSTS=http://elasticsearch:9200 \
  docker.elastic.co/kibana/kibana:8.11.0


sleep 30

# To run the Python services locally

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
  export LOGGER_ES_HOST=localhost:9200
  export LOGGER_INDEX=muezzin_logs
  python main.py

# Health check for Elasticsearch
curl -s http://localhost:9200/_cluster/health | jq '.' 2>/dev/null || echo "Elasticsearch not ready yet"


