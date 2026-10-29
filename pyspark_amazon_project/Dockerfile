
FROM python:3.12-slim

# Install Java for PySpark
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless && \
    rm -rf /var/lib/apt/lists/*

# Configure Java
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

# Install PySpark
RUN pip install --no-cache-dir pyspark==4.2.0

# Copy the project files
COPY main.py .
COPY amazon.csv .

CMD ["python", "main.py"]
