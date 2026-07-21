FROM registry.internal:5000/usdt.hdp34
RUN pip install fastapi "uvicorn[standard]"
CMD ["bash", "-c", "set -a; . /etc/environment; set +a; export HADOOP_USER_NAME=bigred; exec python3 /app/api_server_3.py"]
