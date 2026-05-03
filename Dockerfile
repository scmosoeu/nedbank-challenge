FROM nedbank-de-challenge/base:1.0

# Install any additional Python dependencies you need beyond the base image.
# Leave requirements.txt empty if the base packages are sufficient.
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline code and configuration into the image.
# Do NOT copy data files or output directories — these are injected at runtime
# via Docker volume mounts by the scoring system.
COPY pipeline/ pipeline/
COPY config/ config/
ENV PYTHONPATH="/app"

# Spark’s startup scripts expect the ps utility (from procps), but many slim images (Alpine, distroless, slim Python) don’t include it.
RUN apt-get update && apt-get install -y procps

# Force Spark to use localhost (simplest)
ENV SPARK_LOCAL_IP=127.0.0.1
ENV SPARK_LOCAL_HOSTNAME=localhost

# Update SPARK_HOME path to include "site-packages" since it could not find "dist-packages"
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH=$SPARK_HOME/bin:$PATH

# Add Delta Lake JARs manually to enable delta in spark session
ADD https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar /usr/local/lib/python3.11/site-packages/delta/jars/
ADD https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar /usr/local/lib/python3.11/site-packages/delta/jars/

# Entry point — must run the complete pipeline end-to-end without interactive input.
# The scoring system uses this CMD directly; do not require TTY or stdin.
CMD ["python", "pipeline/run_all.py"]
