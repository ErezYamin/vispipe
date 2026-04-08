# VisPipe

A distributed image preprocessing pipeline built on the Medallion architecture (Bronze, Silver, Gold) using RabbitMQ, OpenCV, MinIO, and PostgreSQL, all running in Docker.

## Stack

| Component | Technology | Purpose |
|---|---|---|
| Message broker | RabbitMQ | Decouples services, routes to DLQ on failure |
| Object storage | MinIO (S3-compatible) | Bronze (raw) and Silver (processed) buckets |
| Database | PostgreSQL | Gold layer structured metadata |
| Image processing | OpenCV | Resize, grayscale, normalize |
| Containerization | Docker Compose | Full stack in one command |

## Quick Start

Prerequisites: Docker + Docker Compose ([download here](https://www.docker.com/products/docker-desktop/))

**1. Clone the repo**

```bash
git clone https://github.com/ErezYamin/vispipe.git
cd vispipe
```

**2. Set up environment and start the stack**

```bash
cp .env.example .env
docker compose up --build
```

All 7 services start automatically. Wait about 20 seconds for everything to be ready.

RabbitMQ management: http://localhost:15672 (guest / guest)
MinIO console: http://localhost:9001 (minioadmin / minioadmin)

These are local credentials running on your machine, no account needed.

## Testing the Pipeline

### Option 1 - MinIO console (recommended)

1. Open http://localhost:9001 and log in with `minioadmin` / `minioadmin`
2. Navigate to the `bronze` bucket and upload any `.jpg` image from your computer
3. Within about 5 seconds, check the `silver` bucket and a preprocessed PNG will appear

### Option 2 - Interactive demo

```bash
pip3 install -r requirements.txt
python3 demo.py
```

Walks through each layer step by step. Saves a before/after comparison image (`demo_comparison.png`).

### Option 3 - Stats CLI

```bash
pip3 install -r cli/requirements.txt
python3 cli/stats.py
```

### Check Gold layer (PostgreSQL)

```bash
docker compose exec postgres psql -U vispipe -d vispipe -c "SELECT image_id, status, width, height, processed_at FROM images ORDER BY processed_at DESC LIMIT 10;"
```

## Pipeline Flow

```
Raw Image -> bronze/ bucket
    -> Ingestor detects file, publishes to raw_images queue
    -> Preprocessor: corrupt check -> resize 224x224 -> grayscale -> normalize [0,1]
    -> Saves to silver/ as PNG
    -> Metadata Extractor writes to PostgreSQL (Gold layer)

Corrupt/failed images -> Dead Letter Queue -> DLQ Inspector logs them
```

## Project Structure

```
services/
  ingestor/           # Polls MinIO bronze, publishes to RabbitMQ
  preprocessor/       # Consumes queue, runs OpenCV pipeline, writes to silver
  metadata_extractor/ # Consumes processed_images queue, writes Gold record to PostgreSQL
  dlq_inspector/      # Logs dead-letter messages
cli/                  # stats.py - queries Gold layer
db/                   # init.sql - schema
sample_images/        # Sample images for testing
tests/                # 27 unit tests (no Docker required)
```

## Running Tests

Unit tests run without Docker:

```bash
pip3 install -r tests/requirements.txt
cd tests && pytest -v
```

## Medallion Layers

| Layer | Storage | Contents |
|---|---|---|
| Bronze | MinIO `bronze/` | Raw input images (JPEG) |
| Silver | MinIO `silver/` | Preprocessed 224x224 grayscale PNGs |
| Gold | PostgreSQL `images` | Metadata: UUID, dimensions, checksum, status, timestamps |
