FROM apache/airflow:2.7.0-python3.9

USER root

# Install Java 11, netcat, Docker CLI, and Kerberos development packages for Spark connectivity and HDFS
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk netcat libkrb5-dev build-essential \
        apt-transport-https ca-certificates curl gnupg lsb-release && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && \
    apt-get install -y docker-ce-cli && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Create docker group and add airflow user to it for Docker socket access
RUN groupadd docker || true && usermod -aG docker airflow

# Install Apache Spark
ENV SPARK_VERSION=3.3.0
ENV HADOOP_VERSION=3.3.1
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Download and install Spark
RUN curl -O https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    chown -R airflow:root ${SPARK_HOME}

USER airflow

# Install Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Install Airflow providers (simplified to avoid complex dependencies)
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.1.3 \
    apache-airflow-providers-postgres==5.5.1 \
    apache-airflow-providers-http==4.5.1

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
USER root
RUN chmod +x /entrypoint.sh
USER airflow

# Set entrypoint
ENTRYPOINT ["/entrypoint.sh"]
