FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1

# Java
RUN apt-get update -y
RUN apt-get install -y openjdk-11-jdk
RUN apt-get install -y kafkacat

# Set up alias
RUN cd /usr/bin && ln -s kafkacat kcat

# Install pip requirements
COPY ./consumer/requirements.txt .
RUN python3 -m pip install -r requirements.txt

WORKDIR /app
COPY ./consumer .
COPY ./scripts/kafka-wait.py .

CMD ["python", "stream.py"]
