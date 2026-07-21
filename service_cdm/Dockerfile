FROM python:3.10

ENV KAFKA_HOST=rc1a-p27s7l68dghcf6i0.mdb.yandexcloud.net
ENV KAFKA_PORT=9091

ENV KAFKA_CONSUMER_USERNAME=producer_consumer
ENV KAFKA_CONSUMER_PASSWORD=TOPAZ540
ENV KAFKA_CONSUMER_GROUP=cdm-consumer-group

ENV KAFKA_SOURCE_TOPIC=dds-service-orders

ENV PG_WAREHOUSE_HOST=rc1b-4u7d1q84rb05820d.mdb.yandexcloud.net
ENV PG_WAREHOUSE_PORT=6432
ENV PG_WAREHOUSE_DBNAME=sprint9dwh
ENV PG_WAREHOUSE_USER=db_user
ENV PG_WAREHOUSE_PASSWORD=TOPPOTZ90


RUN apt-get update -y

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /usr/local/share/ca-certificates/Yandex/

COPY certs/YandexInternalRootCA.crt \
/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt


CMD ["python3", "app.py"]
