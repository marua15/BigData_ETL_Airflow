# Use the Airflow base image
FROM apache/airflow:2.10.3

# Switch to root user to install dependencies
USER root

# Install curl, zip, and unzip
RUN apt-get update && apt-get install -y curl zip unzip

# Install SDKMAN! and required tools (Java 8 and Maven)
RUN curl -s 'https://get.sdkman.io' | bash
RUN /bin/bash -c "source $HOME/.sdkman/bin/sdkman-init.sh; sdk version; sdk install java 8.0.302-open; sdk install maven 3.8.6"

# Set environment variables to use SDKMAN! installed Java and Maven
RUN echo "source $HOME/.sdkman/bin/sdkman-init.sh" >> ~/.bashrc
RUN echo "export PATH=\$PATH:/root/.sdkman/candidates/java/current/" >> ~/.bashrc
# Switch back to the airflow user
USER airflow

# Install PySpark and Airflow provider for Spark
RUN pip install pyspark
RUN pip install apache-airflow-providers-apache-spark



