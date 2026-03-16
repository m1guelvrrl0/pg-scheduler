FROM python:3.12-slim

# Install system dependencies first
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy and install the package (editable so volume mounts work at runtime)
COPY . .
RUN pip install -e .

CMD ["python", "example.py"]