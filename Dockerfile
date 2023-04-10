# Use an official Python runtime as a parent image
FROM python:3.8-slim-buster

# Set the working directory to /app/jupyter
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y wget curl postgresql-client libpq-dev build-essential

# Copy the requirements file
COPY requirements.txt /app


# Install Python dependencies
RUN pip install --no-cache-dir -r /app/requirements.txt

# Install Java
RUN apt-get install -y openjdk-11-jre-headless

# Install Apache Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz && \
    tar -xf spark-3.2.0-bin-hadoop3.2.tgz && \
    mv spark-3.2.0-bin-hadoop3.2 /usr/local/spark && \
    rm spark-3.2.0-bin-hadoop3.2.tgz

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$JAVA_HOME/bin
#ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYARROW_IGNORE_TIMEZONE=1


# Copy the project code
COPY . /app
COPY postgresql-42.6.0.jar /usr/local/spark/jars

# # Copy scripts from src directory
# COPY src/ /app/jupyter/scripts/

# # Copy notebooks from Notebooks directory
# COPY Notebooks/ /app/jupyter/notebooks/


# Expose the Jupyter notebook port
EXPOSE 8888

# Start Jupyter notebook
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--allow-root", "--NotebookApp.token=''", "--no-browser"]
