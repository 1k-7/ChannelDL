FROM python:3.11-slim

# Install system dependencies (gcc is needed for tgcrypto optimization)
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Ensure the database persists by using a volume in docker-compose
CMD ["python", "bot.py"]
