import os


class AppConfig:
    def __init__(self):
        # Kafka
        self.kafka_host = os.getenv(
            'KAFKA_HOST',
            'rc1a-p27s7l68dghcf6i0.mdb.yandexcloud.net'
        )
        self.kafka_port = int(os.getenv('KAFKA_PORT', 9091))

        self.kafka_consumer_username = os.getenv(
            'KAFKA_CONSUMER_USERNAME',
            'producer_consumer'
        )
        self.kafka_consumer_password = os.getenv(
            'KAFKA_CONSUMER_PASSWORD',
            'TOPAZ540'
        )

        self.kafka_consumer_group = os.getenv(
            'KAFKA_CONSUMER_GROUP',
            'cdm-consumer-group-v3'
        )

        self.kafka_source_topic = os.getenv(
            'KAFKA_SOURCE_TOPIC',
            'dds-service-orders'
        )

        self.kafka_cert_path = (
            '/usr/local/share/ca-certificates/Yandex/'
            'YandexInternalRootCA.crt'
        )

        # PostgreSQL
        self.pg_warehouse_host = os.getenv(
            'PG_WAREHOUSE_HOST',
            'rc1b-4u7d1q84rb05820d.mdb.yandexcloud.net'
        )
        self.pg_warehouse_port = int(
            os.getenv('PG_WAREHOUSE_PORT', 6432)
        )
        self.pg_warehouse_dbname = os.getenv(
            'PG_WAREHOUSE_DBNAME',
            'sprint9dwh'
        )
        self.pg_warehouse_user = os.getenv(
            'PG_WAREHOUSE_USER',
            'db_user'
        )
        self.pg_warehouse_password = os.getenv(
            'PG_WAREHOUSE_PASSWORD',
            'TOPPOTZ90'
        )
