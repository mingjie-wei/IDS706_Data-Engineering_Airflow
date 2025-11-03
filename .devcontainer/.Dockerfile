# 1. Start from the official Airflow image
FROM apache/airflow:3.1.0

# 2. Switch to root user to install system-level packages
USER root

# 3. Install OpenJDK 17 for PySpark
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    # create arch-agnostic symlink: java-17-openjdk -> java-17-openjdk-<arch>
    && ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17-openjdk || true

# 4. Set JAVA_HOME to the symlink so it works on amd64/arm64
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# 5. Back to non-root airflow user
USER airflow

# 6. Install Python deps
COPY --chown=airflow:root requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
