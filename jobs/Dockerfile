FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY demo_ingest.py real_ingest.py wallet_discovery.py price_ingest.py graph_similarity.py token_paths.py .

CMD ["python", "demo_ingest.py"]
