import json
import time
from logging import Logger
from typing import Dict, Optional

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from stg_loader.repository.stg_repository import StgRepository


class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis_client: RedisClient,
                 stg_repository: StgRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis_client
        self._stg_repository = stg_repository
        self._batch_size = batch_size
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{self.__class__.__name__} started")
        processed_count = 0

        for _ in range(self._batch_size):
            # 1. Получить сообщение из Kafka
            msg = self._consumer.consume()
            if msg is None:
                self._logger.info("No more messages to process")
                break

            try:
                # 2. Сохранить сырое сообщение в STG
                self._stg_repository.order_events_insert(
                    object_id=msg['object_id'],
                    object_type=msg['object_type'],
                    sent_dttm=msg['sent_dttm'],
                    payload=json.dumps(msg['payload'])
                )

                # 3. Обогатить данными из Redis
                user_id = msg['payload']['user']['id']
                user_data = self._redis.get(user_id)

                restaurant_id = msg['payload']['restaurant']['id']
                restaurant_data = self._redis.get(restaurant_id)

                self._logger.info(
                    f"STG RESTAURANT DEBUG id={restaurant_id} data={restaurant_data}"
                )

                # 4. Обогатить продукты из меню ресторана
                menu = restaurant_data.get('menu', []) if restaurant_data else []

                self._logger.info(
                    f"STG MENU DEBUG count={len(menu)} first={menu[:1]}"
                )
                menu_by_id = {
                    item.get('_id'): item
                    for item in menu
                    if item.get('_id')
                }

                enriched_products = []
                for product in msg['payload']['order_items']:
                    product_id = product['id']
                    product_data = menu_by_id.get(product_id)

                    self._logger.info(
                        f"STG PRODUCT MATCH id={product_id} found={product_data is not None}"
                    )

                    enriched_products.append({
                        'id': product_id,
                        'price': product['price'],
                        'quantity': product['quantity'],
                        'name': product_data.get('name') if product_data else product.get('name', 'unknown'),
                        'category': product_data.get('category') if product_data else 'unknown'
                    })

                # 5. Собрать выходное сообщение
                enriched_message = {
                    'object_id': msg['object_id'],
                    'object_type': msg['object_type'],
                    'payload': {
                        'id': msg['object_id'],
                        'date': msg['payload']['date'],
                        'cost': msg['payload']['cost'],
                        'payment': msg['payload']['payment'],
                        'status': msg['payload']['final_status'],
                        'restaurant': {
                            'id': restaurant_id,
                            'name': restaurant_data.get('name') if restaurant_data else 'unknown'
                        },
                        'user': {
                            'id': user_id,
                            'name': user_data.get('name') if user_data else 'unknown'
                        },
                        'products': enriched_products
                    }
                }

                # 6. Отправить обогащённое сообщение в Kafka
                self._producer.produce(enriched_message)

                processed_count += 1

                # Логирование прогресса
                if processed_count % 10 == 0:
                    self._logger.info(f"Processed {processed_count} messages")

            except Exception as e:
                self._logger.exception("Error processing message")
                continue

        self._logger.info(f"{self.__class__.__name__} finished. Processed {processed_count} messages")
