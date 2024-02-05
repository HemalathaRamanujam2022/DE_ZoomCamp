# Specify that the docker env is Python
FROM python:3.9

RUN apt-get install wget
# Install any Python packages
RUN pip install pandas sqlalchemy psycopg2 

# Create a working directory to copy the application files from dev env to docker env
WORKDIR /app
# COPY pipeline.py pipeline.py
COPY ingest_data.py ingest_data.py

# This will be the first command which will be shown once the docker file is run
# ENTRYPOINT ["bash"]
# ENTRYPOINT ["python", "pipeline.py"]
ENTRYPOINT ["python", "ingest_data.py"]