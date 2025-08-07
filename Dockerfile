FROM python:3.11-alpine

# Install system dependencies
RUN apk add --no-cache \
    postgresql-dev \
    gcc \
    musl-dev \
    tzdata

# Set timezone
ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Create app directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY job.py .

# Create non-root user
RUN addgroup -g 1001 -S scorer && \
    adduser -S -D -H -u 1001 -s /sbin/nologin -G scorer scorer

# Create log directory
RUN mkdir -p /tmp && chown scorer:scorer /tmp

USER scorer

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python -c "import psycopg2; print('OK')" || exit 1

CMD ["python", "job.py"]
