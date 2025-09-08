FROM python:3.11-slim

WORKDIR /app

COPY . /app

# Update system packages to address vulnerabilities
RUN apt-get update && apt-get upgrade -y && apt-get clean && \
	pip install --no-cache-dir -r requirements.txt

HEALTHCHECK CMD curl --fail http://localhost:8000/health || exit 1
	
ENV PYTHONPATH=/app

CMD ["python", "main.py"]
