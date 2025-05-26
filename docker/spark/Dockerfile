FROM bde2020/spark-master:3.3.0-hadoop3.3

USER root

# Install Python pip and required system packages (Alpine Linux)
RUN apk update && \
    apk add --no-cache python3 py3-pip python3-dev build-base gcc musl-dev linux-headers

# Copy and install Python requirements
COPY requirements.txt /opt/spark/requirements.txt
RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir -r /opt/spark/requirements.txt

# Set Python path for Spark
ENV PYTHONPATH="/spark/python:/spark/python/lib/pyspark.zip:/spark/python/lib/py4j-*.zip:$PYTHONPATH"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

USER spark
