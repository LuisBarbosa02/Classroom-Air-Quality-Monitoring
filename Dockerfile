# Set Docker environment
FROM python:3.12.12-slim

# Preventing Python from writing .pyc files
ENV PYTHONDONTWRITEBYTECODE=1

# Send Python output to the Docker logs
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Copy necessary files
COPY requirements.txt ./
COPY app/ ./app/
COPY data/ ./data/
COPY .env ./

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run command
CMD ["python", "-m", "app.consumer"]