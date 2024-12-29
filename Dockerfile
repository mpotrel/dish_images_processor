FROM python:3.11-slim
RUN pip install poetry
WORKDIR /app
COPY pyproject.toml poetry.lock ./
COPY dish_images_processor ./dish_images_processor
# Disable virtualenv creation for Docker
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction
EXPOSE 8000
CMD ["poetry", "run", "uvicorn", "dish_images_processor.app:app", "--host", "0.0.0.0", "--port", "8000"]
