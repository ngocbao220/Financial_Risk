FROM python:3.10-slim
WORKDIR /app
COPY app/producer.py /app/producer.py
RUN pip install -r requirements.txt
CMD ["python", "/app/producer.py"]
