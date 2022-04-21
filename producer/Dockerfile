FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1

RUN apt-get update -y
RUN apt-get install -y kafkacat

# Install pip requirements
COPY ./producer/requirements.txt .
RUN python3 -m pip install -r requirements.txt

WORKDIR /app
COPY ./producer/app .
COPY ./scripts/kafka-wait.py .
RUN cd /usr/bin && ln -s kafkacat kcat

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]
