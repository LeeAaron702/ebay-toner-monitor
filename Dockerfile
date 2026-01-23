# Use the official Python image from the Docker Hub
FROM python:3.13-slim

# Set the working directory in the container
WORKDIR /app

# Copy requirements.txt first to leverage Docker cache
COPY requirements.txt ./

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright dependencies for headless Chromium + git for sync
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libcairo2 \
    git \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

# Install Playwright Chromium browser
RUN playwright install chromium

# Copy the rest of the application code
COPY . .

# Set environment variables (optional, can be customized)
ENV PYTHONUNBUFFERED=1


# Expose FastAPI port
EXPOSE 8000

# Start FastAPI server with uvicorn
CMD ["uvicorn", "api.api_server:app", "--host", "0.0.0.0", "--port", "8000"]
