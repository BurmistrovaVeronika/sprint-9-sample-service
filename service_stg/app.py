import logging
import time

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.pg import PgConnect
from lib.redis import RedisClient
from stg_loader.repository.stg_repository import StgRepository
from stg_loader.stg_message_processor_job import StgMessageProcessor
from config import AppConfig


def main():
    # Настройка логирования
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Загрузка конфигурации
    config = AppConfig()

    # Инициализация зависимостей
    consumer = KafkaConsumer(
        host=config.kafka_host,
        port=config.kafka_port,
        user=config.kafka_consumer_username,
        password=config.kafka_consumer_password,
        topic=config.kafka_source_topic,
        group=config.kafka_consumer_group,
        cert_path=config.kafka_cert_path
    )

    producer = KafkaProducer(
        host=config.kafka_host,
        port=config.kafka_port,
        user=config.kafka_producer_username,
        password=config.kafka_producer_password,
        topic=config.kafka_destination_topic,
        cert_path=config.kafka_cert_path
    )

    redis_client = RedisClient(
        host=config.redis_host,
        port=config.redis_port,
        password=config.redis_password,
        cert_path=config.redis_cert_path
    )

    pg_connect = PgConnect(
        host=config.pg_warehouse_host,
        port=config.pg_warehouse_port,
        db_name=config.pg_warehouse_dbname,
        user=config.pg_warehouse_user,
        pw=config.pg_warehouse_password
    )

    stg_repository = StgRepository(pg_connect)

    batch_size = 100

    # Создание экземпляра StgMessageProcessor
    processor = StgMessageProcessor(
        consumer=consumer,
        producer=producer,
        redis_client=redis_client,
        stg_repository=stg_repository,
        batch_size=batch_size,
        logger=logger
    )

    while True:
        try:
            processor.run()
            logger.info("Batch processed. Waiting for new messages...")
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            break
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(10)


if __name__ == "__main__":
    main()
