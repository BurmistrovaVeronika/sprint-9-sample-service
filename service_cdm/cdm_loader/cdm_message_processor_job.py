import time
from logging import Logger
from typing import Dict

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:

    def __init__(
        self,
        consumer: KafkaConsumer,
        cdm_repository: CdmRepository,
        batch_size: int,
        logger: Logger
    ) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._batch_size = batch_size
        self._logger = logger


    def run(self) -> None:
        self._logger.info(
            f"{self.__class__.__name__} started"
        )

        processed_count = 0

        for _ in range(self._batch_size):

            msg = self._consumer.consume()

            if msg is None:
                self._logger.info(
                    "No more messages to process"
                )
                break

            try:
                self._process_message(msg)
                processed_count += 1

            except Exception as e:
                self._logger.error(
                    f"Error processing message: {e}"
                )
                continue


        self._logger.info(
            f"{self.__class__.__name__} finished. "
            f"Processed {processed_count} messages"
        )


    def _process_message(
        self,
        msg: Dict
    ) -> None:

        self._logger.info(f"CDM MESSAGE DEBUG: {msg}")

        user_id = msg["user_id"]

        products = msg.get(
            "products",
            []
        )


        for product in products:

            product_id = product["product_id"]

            product_name = product.get(
                "product_name",
                "unknown"
            )

            category_name = product.get(
                "category_name",
                "unknown"
            )


            # product витрина
            self._cdm_repository.upsert_user_product_counter(
                user_id=user_id,
                product_id=product_id,
                product_name=product_name
            )


            # category витрина
            category_id = (
                self._cdm_repository
                .get_category_id(category_name)
            )


            if category_id:

                self._cdm_repository.upsert_user_category_counter(
                    user_id=user_id,
                    category_id=str(category_id),
                    category_name=category_name
                )


        self._logger.info(
            f"Processed CDM order {msg['order_id']}"
        )
