FROM python:3.8-slim

WORKDIR /app

COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Set the environment variable for the Cloud Run service
ENV PORT 8080

# Run the application
CMD python main.py
