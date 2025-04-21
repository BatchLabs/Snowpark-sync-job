FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script
COPY batch_sync.py .

# Set the entrypoint
ENTRYPOINT ["python", "batch_sync.py"]