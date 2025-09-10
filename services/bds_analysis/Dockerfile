FROM python:3.11-slim

WORKDIR /app

# Copy shared directory first
COPY shared ./shared

# Copy requirements first for better caching
COPY services/bds_analysis/requirements.txt ./

# Update system packages to address vulnerabilities
RUN apt-get update && apt-get upgrade -y && apt-get clean && \
	pip install --no-cache-dir -r requirements.txt

# Copy the service files
COPY services/bds_analysis ./

# Set Python path
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]


