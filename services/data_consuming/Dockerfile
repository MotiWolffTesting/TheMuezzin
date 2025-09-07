FROM python:3.11-slim

WORKDIR /app

COPY . /app

# Update system packages to address vulnerabilities
RUN apt-get update && apt-get upgrade -y && apt-get clean && \
	pip install --no-cache-dir -r requirements.txt

CMD ["python", "-m", "main.py"]
