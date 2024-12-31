# Dish Image Processor

A scalable image processing pipeline using fal.ai APIs and Kafka for message streaming.

## Prerequisites
- Docker and Docker Compose
- Python 3.11+

## Quick Start

1. Clone the repository
```bash
git clone <repository-url>
cd dish-images-processor
```

Create a `.env` file containing your settings in the following format:

```
FAL_KEY = <YOUR_API_KEY>
KAFKA_BOOTSTRAP_SERVERS = <SINGLE KAFKA BOOTSTRAP SERVER (FOR NOW)>
MAX_CONCURRENT_REQUESTS = <INTEGER>
```

Though it is possible to run the package directly on your machine, provided you have the necessary servers running, it is recommended to use the `docker-compose.yml` file instead.
Run `docker compose up -d` to start the application.

# Usage
You may now submit a list of images using `curl` or `requests` or any other HTTP request utility you like. Here is an example:
```bash
curl -X 'POST' \
  'http://localhost:8000/v0/preprocess' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "images": [
    "link_to_image_1",
    "link_to_image_2",
    ...
  ]}'
```

# Monitoring and Verification
## Checking Service Status

View logs: `docker compose logs -f app`
Monitor Kafka topics: `docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list`
View messages in a topic: `docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic <topic-name> --from-beginning`

## Verifying Concurrency Limits

Submit a large batch of images (10+)
Monitor logs to see concurrent processing:

```bash
docker compose logs -f app | grep "Remaining slots"
```

# Development Mode
To run without making actual fal.ai API calls:

Enter the container:

```bash
docker exec -it dish-images-processor-app-1
```

Stop the existing process and start in debug mode:

```bash
kill $(pgrep -f uvicorn)
DEBUG_MODE=true poetry run uvicorn dish_images_processor.app:app --host 0.0.0.0 --port 8000
```

# Running unit tests
You can run unit tests with `docker exec -it <app-container> poetry run pytest tests/ -v`

# Contributing (project structure)
The `dish_images_processor` package is structured as follows:
- `config` contains all the configuration for the app, including `logging`, `fastapi` settings but also the `fal` service configuration variables, such as the `fal` endpoint for each service and the associated arguments.
- `services` contains the services python files. At the moment, all services have the same logic and can be distinguished simply by their `yaml` config. While services and consumers have a one to one relationship, it is cleaner to encapsulate all `kafka` consumers logic elsewhere, so services only contain requests and processing logic.
- `kafka` defines the consumer and producer classes, as well as the topics definitions.
- `routers` is where the different routers for the API are defined. They should be versioned, and prod and dev routes should be distinct. For example, the prod v3 version's route would be "/v3/prod/process".
- `models` is also associated with `fastapi` since the requests models are defined there.
- `utils` currently only contains concurrency code.

Note that all Kafka related setup is run at startup in the `app.py` file using functions from `dish_images_processor.kafka` (topic creation, producers and consumers instanciation)
