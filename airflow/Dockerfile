FROM apache/airflow:2.10.4

# Install Snowflake provider AND the required visualization libraries 
# in the main Airflow environment
RUN pip install --no-cache-dir \
    "apache-airflow-providers-snowflake" \
    "pandas" \
    "plotly" \
    "apache-airflow-providers-amazon"

# Create a separate virtual environment JUST for dbt to avoid conflicts!
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-snowflake

# Switch to root to create a global symlink for the dbt command
USER root
RUN ln -s /opt/airflow/dbt_venv/bin/dbt /usr/local/bin/dbt

# Switch back to the default airflow user (UID 50000) for security
USER airflow
