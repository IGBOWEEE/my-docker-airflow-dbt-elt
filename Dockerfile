# Dockerfile

# --- IMPORTANT ---
# Use the *exact same* Airflow version as the docker-compose.yaml you downloaded
# You downloaded the compose file for 2.10.5, so use that tag here:
    FROM apache/airflow:2.10.5

    # Switch to root user to install system packages
    USER root
    
    # Install system dependencies (curl and unzip were needed for SnowSQL,
    # you might remove them if ONLY using Python, but they are small and harmless)
    RUN apt-get update && \
        apt-get install -y --no-install-recommends \
            curl \
            unzip \
        && \
        apt-get clean && \
        rm -rf /var/lib/apt/lists/*
    
    # No longer installing SnowSQL here as we are using the Python direct load method.
    
    # Temporarily stay as root to install pip packages globally
    # USER root # Already root from previous step
    
    # Install Python dependencies required by your DAGs and scripts
    # Pinning versions is good practice for reproducibility
    # Check PyPI for latest compatible versions if desired
USER airflow

# Install Python dependencies required by your DAGs and scripts
# Pinning versions is good practice for reproducibility
# Check PyPI for latest compatible versions if desired
RUN pip install --no-cache-dir \
    apache-airflow-providers-snowflake==5.4.0 \
    snowflake-connector-python[pandas]==3.8.0 \
    snowflake-sqlalchemy==1.5.1 \
    dbt-core==1.7.13 \
    dbt-snowflake==1.7.4 \
    pandas==2.1.4 \
    Faker==25.0.0
    # Add any OTHER python libraries imported by your custom scripts here

# No need to switch user again, already airflow
# USER airflow