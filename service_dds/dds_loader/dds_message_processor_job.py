import json
import time
from logging import Logger
from typing import Dict, Optional

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from dds_loader.repository.dds_repository import DdsRepository


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis_client: RedisClient,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis_client
        self._dds_repository = dds_repository
        self._batch_size = batch_size
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{self.__class__.__name__} started")
        processed_count = 0

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if msg is None:
                self._logger.info("No more messages to process")
                break

            try:
                self._process_message(msg)
                processed_count += 1

                if processed_count % 10 == 0:
                    self._logger.info(f"Processed {processed_count} messages")

            except Exception as e:
                self._logger.error(f"Error processing message: {e}")
                continue

        self._logger.info(f"{self.__class__.__name__} finished. Processed {processed_count} messages")

    def _process_message(self, msg: Dict) -> None:
        payload = msg["payload"]

        order_id = msg["object_id"]
        order_dt = payload["date"]
        cost = payload["cost"]
        payment = payload["payment"]
        status = payload["status"]

        user = payload["user"]
        user_id = user["id"]
        username = user.get("name", "unknown")
        userlogin = user.get("login", "unknown")

        restaurant = payload["restaurant"]
        restaurant_id = restaurant["id"]
        restaurant_name = restaurant.get("name", "unknown")

        products = payload["products"]
        self._logger.info(f"DDS PRODUCTS SAMPLE: {products[:1]}")
        load_src = "kafka"

        h_order_pk = self._dds_repository.insert_hub_order(
            order_id, order_dt, load_src
        )
        h_user_pk = self._dds_repository.insert_hub_user(
            user_id, load_src
        )
        h_restaurant_pk = self._dds_repository.insert_hub_restaurant(
            restaurant_id, load_src
        )

        self._dds_repository.insert_link_order_user(
            h_order_pk, h_user_pk, load_src
        )

        self._dds_repository.insert_sat_order_cost(
            h_order_pk, cost, payment, load_src
        )
        self._dds_repository.insert_sat_order_status(
            h_order_pk, status, load_src
        )
        self._dds_repository.insert_sat_user_names(
            h_user_pk, username, userlogin, load_src
        )
        self._dds_repository.insert_sat_restaurant_names(
            h_restaurant_pk, restaurant_name, load_src
        )

        for product in products:
            product_id = product["id"]
            product_name = product.get("name", "unknown")
            category_name = product.get("category", "unknown")

            h_product_pk = self._dds_repository.insert_hub_product(
                product_id, load_src
            )
            h_category_pk = self._dds_repository.insert_hub_category(
                category_name, load_src
            )

            self._dds_repository.insert_link_order_product(
                h_order_pk, h_product_pk, load_src
            )
            self._dds_repository.insert_link_product_category(
                h_product_pk, h_category_pk, load_src
            )
            self._dds_repository.insert_link_product_restaurant(
                h_product_pk, h_restaurant_pk, load_src
            )
            self._dds_repository.insert_sat_product_names(
                h_product_pk, product_name, load_src
            )

        dds_message = {
            "order_id": order_id,
            "user_id": user_id,
            "products": [
                {
                    "product_id": product["id"],
                    "product_name": product.get("name", "unknown"),
                    "category_name": product.get("category", "unknown")
                }
                for product in products
            ],
            "timestamp": time.time(),
        }

        self._producer.produce(dds_message)
        self._logger.info(f"Sent DDS message for order {order_id}")
