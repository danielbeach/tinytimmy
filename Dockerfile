FROM python:latest

RUN pip install poetry

# Copy only requirements to cache them in docker layer
WORKDIR /app
COPY pyproject.toml /app/

# Project initialization:
RUN poetry config virtualenvs.create false \
  && poetry install --no-interaction --no-ansi

# Creating folders, and files for a project:
COPY . /app