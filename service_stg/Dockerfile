FROM python:3.10

ENV KAFKA_HOST=rc1a-p27s7l68dghcf6i0.mdb.yandexcloud.net
ENV KAFKA_PORT=9091
ENV KAFKA_USER=producer_consumer
ENV KAFKA_PASSWORD=В1LRTNca54
ENV KAFKA_TOPIC=order-service_orders
ENV KAFKA_DESTINATION_TOPIC=stg-service-orders

ENV REDIS_HOST=rc1b-ak8sflhrqvbfrnre.mdb.yandexcloud.net
ENV REDIS_PORT=6380
ENV REDIS_PASSWORD=ZAPOT0s54
ENV REDIS_CERT_PATH=/home/burmi/.redis/YandexInternalRootCA.crt

ENV PG_HOST=rc1b-4u7d1q84rb05820d.mdb.yandexcloud.net
ENV PG_PORT=6432
ENV PG_DBNAME=sprint9dwh
ENV PG_USER=db_user
ENV PG_PASSWORD=TOPPOTZ90

RUN apt-get update -y

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
COPY certs/YandexInternalRootCA.crt /usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt
CMD ["python", "app.py"]
