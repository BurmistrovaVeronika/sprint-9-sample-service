import logging
import time

from config import AppConfig

from lib.kafka_connect import KafkaConsumer
from lib.pg import PgConnect

from cdm_loader.repository.cdm_repository import CdmRepository
from cdm_loader.cdm_message_processor_job import CdmMessageProcessor


def main():

    logging.basicConfig(
        level=logging.INFO
    )

    logger = logging.getLogger(__name__)


    config = AppConfig()


    consumer = KafkaConsumer(
        host=config.kafka_host,
        port=config.kafka_port,
        user=config.kafka_consumer_username,
        password=config.kafka_consumer_password,
        topic=config.kafka_source_topic,
        group=config.kafka_consumer_group,
        cert_path=config.kafka_cert_path
    )


    pg_connect = PgConnect(
        host=config.pg_warehouse_host,
        port=config.pg_warehouse_port,
        db_name=config.pg_warehouse_dbname,
        user=config.pg_warehouse_user,
        pw=config.pg_warehouse_password
    )


    repository = CdmRepository(
        pg_connect
    )


    processor = CdmMessageProcessor(
        consumer=consumer,
        cdm_repository=repository,
        batch_size=100,
        logger=logger
    )


    while True:

        processor.run()

        logger.info(
            "Batch processed. Waiting for new messages..."
        )

        time.sleep(3)


if __name__ == "__main__":
    main()
