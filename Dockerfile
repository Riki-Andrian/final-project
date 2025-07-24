FROM apache/airflow:2.10.0-python3.10

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

# COPY load_data.py .
# COPY data/ ./data/

# CMD ["python", "load_data.py"]
