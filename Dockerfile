# Simple Docker image to run unit tests in a clean environment
FROM python:3.11-slim

# Avoid python writing .pyc, and force unbuffered logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Copy only dependency files first (better caching)
COPY lambdas/requirements.txt /app/lambdas/requirements.txt
COPY requirements-dev.txt /app/requirements-dev.txt
COPY pytest.ini /app/pytest.ini

# Install dependencies
RUN python -m pip install --upgrade pip \
    && pip install -r /app/lambdas/requirements.txt \
    && pip install -r /app/requirements-dev.txt

# Copy the rest of the repo
COPY . /app

# Default command: run tests
CMD ["pytest", "-q"]
