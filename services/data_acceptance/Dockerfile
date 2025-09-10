FROM python:3.11-slim

WORKDIR /app

# Copy shared directory first
COPY shared ./shared

# Copy requirements first for better caching
COPY services/data_acceptance/requirements.txt ./

# Update system packages to address vulnerabilities
RUN apt-get update && apt-get upgrade -y && apt-get clean && \
	pip install --no-cache-dir -r requirements.txt

# Copy the service files
COPY services/data_acceptance ./

# Set Python path
ENV PYTHONPATH=/app

CMD ["python", "main.py"]
